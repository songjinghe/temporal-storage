package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import org.act.dynproperty.Level;
import org.act.dynproperty.impl.MemTable.MemTableIterator;
import org.act.dynproperty.table.FileChannelTable;
import org.act.dynproperty.table.MergeProcess;
import org.act.dynproperty.table.Table;
import org.act.dynproperty.table.TableBuilder;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.table.TableIterator;
import org.act.dynproperty.util.Slice;
import org.act.dynproperty.util.TimeIntervalUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UnStableLevel，所有的UnStableFile和MemTable相关信息保存的地方
 */
public class UnstableLevel implements Level
{
	/**
	 * 所有的UnStableFile的元信息
	 */
    private SortedMap<Long,FileMetaData> files;
    /**
     * 所有UnStableFile对应的Buffer
     */
    private SortedMap<Long,FileBuffer> fileBuffers;
    private String dbDir;
    /**
     * MemTable
     */
    private MemTable memTable;
    private MemTable stableMemTable;
    private int memTableBoundary;
    /**
     * 文件合并过程的工具类
     */
    private MergeProcess mergeProcess;
    private static Logger log = LoggerFactory.getLogger( UnstableLevel.class );
    /**
     * UNStableLevel中对文件的缓存
     */
    private TableCache cache;
    private ReentrantLock memtableLock;
    /**
     * 等待写入磁盘的MemTable的队列
     */
    private BlockingQueue<MemTable> mergeWaitingQueue = new LinkedBlockingQueue<MemTable>();
    /**
     * 合并文件的线程
     */
    private Thread mergeThread;
    private volatile boolean mergeIsHappening = false;
    /**
     * 更新和度去文件元信息的读写锁
     */
    private ReadWriteLock fileMetaDataLock;
    private StableLevel stLevel;
    
    UnstableLevel( String dbDir, MergeProcess merge, ReadWriteLock fileLock, StableLevel stLevel )
    {
        this.dbDir = dbDir;
        this.files = new TreeMap<Long,FileMetaData>();
        this.fileBuffers = new TreeMap<Long,FileBuffer>();
        this.mergeProcess = merge;
        cache = new TableCache( new File( dbDir ), 5, TableComparator.instence(), false, false );
        this.memtableLock = new ReentrantLock( true );
        this.fileMetaDataLock = fileLock;
        this.stLevel = stLevel;
        this.mergeThread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                Thread.currentThread().setName("Dynamic-Storage-Merge");
                while(true)
                {
                    try
                    {
                        MemTable temp = mergeWaitingQueue.take();
                        mergeIsHappening = true;
                        UnstableLevel.this.startMergeProcess( temp );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace( );
                        log.error( "error happens when dump memtable to disc", e );
                    }
                    catch ( InterruptedException e )
                    {
                        return;
                    }
                    finally
                    {
                        mergeIsHappening = false;
                    }
                }
            }
        } );
        this.mergeThread.start();
    }
    /**
     * 回复相关文件元信息，判断其是否有Buffer存在。
     */
    public void initfromdisc( FileMetaData metaData )
    {
        try
        {
            this.files.put(metaData.getNumber(), metaData);
            String bufferName = Filename.bufferFileName( metaData.getNumber() );
            File bufferfile = new File(this.dbDir + "/" + bufferName );
            if( bufferfile.exists() )
            {
                FileBuffer buffer = new FileBuffer( bufferName, this.dbDir + "/" + bufferName );
                this.fileBuffers.put( metaData.getNumber(), buffer );
            }
            else
                this.fileBuffers.put( metaData.getNumber(), null );
        }
        catch( IOException e )
        {
            log.error( "Error happens when load bufferfile:" + metaData.getNumber() + " contents!" );
        }
    }
    
    /**
     * 从磁盘中回复MemTable中的数据
     */
    public void restoreMemTable()
    {
        try
        {
            String tempFileName = Filename.tempFileName(0);
            File tempFile = new File( this.dbDir + "/" + tempFileName );
            if( tempFile.exists() )
            {
                FileInputStream inputStream = new FileInputStream(tempFile);
                FileChannel channel = inputStream.getChannel();
                Table table = new FileChannelTable(tempFileName, channel, TableComparator.instence(), false);
                TableIterator iterator = table.iterator();
                if( iterator.hasNext() )
                {
                    this.memTable = new MemTable(TableComparator.instence());
                    while (iterator.hasNext()) {
                        Entry<Slice, Slice> entry = iterator.next();
                        this.memTable.add(entry.getKey(), entry.getValue());
                    }
                    this.memTableBoundary = this.memTable.getStartTime();
                    inputStream.close();
                    channel.close();
                }
                else
                {
                    this.createNewEmptyMemTable();
                }
                Files.delete(tempFile.toPath());
            }
            else
            {
                this.createNewEmptyMemTable();
            }
        }
        catch( IOException e )
        {
            log.error( "Restore MemTable Failed!" );
        }
    }

    private void createNewEmptyMemTable() {
        this.memTable = new MemTable(TableComparator.instence());
        this.memTableBoundary = 0;
        for (Long number : this.files.keySet()) {
            if (this.files.get(number) != null) {
                this.memTableBoundary = this.files.get(number).getLargest() + 1;
                break;
            }
        }
    }

    /**
     * 时间点查询
     */
    @Override
    public Slice getPointValue( Slice idSlice, int time )
    {
        InternalKey searchKey = new InternalKey( idSlice, time, 0, ValueType.VALUE );
        this.fileMetaDataLock.readLock().lock();
        List<Entry<Slice,Slice>> candidats = new LinkedList<Entry<Slice,Slice>>();
        {
            try
            {
                SeekingIterator<Slice,Slice> iterator = this.memTable.iterator();
                iterator.seek( searchKey.encode() );
                Entry<Slice,Slice> entry = iterator.next();
                InternalKey answerKey = new InternalKey( entry.getKey() );
                if( answerKey.getId().equals( idSlice ) && answerKey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId()  )
                {
                    candidats.add( entry );
                }
            }
            catch( NoSuchElementException e){}
            try
            {
                if( this.stableMemTable != null )
                {
                    SeekingIterator<Slice,Slice> iterator = this.stableMemTable.iterator();
                    iterator.seek( searchKey.encode() );
                    Entry<Slice,Slice> entry = iterator.next();
                    InternalKey answerKey = new InternalKey( entry.getKey() );
                    if( answerKey.getId().equals( idSlice ) && answerKey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId()  )
                    {
                        candidats.add( entry );
                    }
                }
            }
            catch( NoSuchElementException e){}
        }
        {
            for( Long fileNumber = 0L; fileNumber < 5; fileNumber++ )
            {
                FileMetaData meta = this.files.get( fileNumber );
                if( null != meta )
                {
                    if( time < meta.getSmallest() )
                        continue;
                    SeekingIterator<Slice,Slice> iterator = this.cache.newIterator( meta );
                    iterator.seek( searchKey.encode() );
                    Entry<Slice,Slice> entry = null;
                    try
                    {
                        entry = iterator.next();
                    }
                    catch(NoSuchElementException e )
                    {
                        FileBuffer buffer = this.fileBuffers.get( fileNumber );
                        if( null != buffer )
                        {
                            SeekingIterator<Slice,Slice> bufferIterator = this.fileBuffers.get( fileNumber ).iterator();
                            bufferIterator.seek( searchKey.encode() );
                            Entry<Slice,Slice> bufferEntry;
                            try
                            {
                                bufferEntry = bufferIterator.next();
                            }
                            catch( NoSuchElementException e2 )
                            {
                                continue;
                            }
                            InternalKey bufferkey = new InternalKey( bufferEntry.getKey() );
                            if( bufferkey.getId().equals( idSlice ) )
                            {
                                if( bufferkey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId() )
                                    candidats.add( bufferEntry );
                            }
                        }
                        continue;
                    }
                    FileBuffer buffer = this.fileBuffers.get( fileNumber );
                    if( null != buffer )
                    {
                        SeekingIterator<Slice,Slice> bufferIterator = this.fileBuffers.get( fileNumber ).iterator();
                        bufferIterator.seek( searchKey.encode() );
                        Entry<Slice,Slice> bufferEntry = null;
                        try
                        {
                            bufferEntry = bufferIterator.next();
                        }
                        catch( NoSuchElementException e3 )
                        {
                            InternalKey key = new InternalKey( entry.getKey() );
                            if( key.getId().equals( idSlice ) )
                            {
                                candidats.add( entry );
                            }
                            continue;
                        }
                        InternalKey bufferkey = new InternalKey( bufferEntry.getKey() );
                        InternalKey key = new InternalKey( entry.getKey() );
                        if( key.getId().equals( idSlice ) )
                        {
                            if( bufferkey.getId().equals( idSlice ) )
                            {
                                if( key.getStartTime() == bufferkey.getStartTime() )
                                {
                                    if( key.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId() )
                                        candidats.add( bufferEntry );
                                }
                                else
                                {
                                    if( key.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId() )
                                        candidats.add( bufferEntry );
                                    candidats.add( entry );
                                }
                            }
                            else
                                candidats.add( entry );
                        }
                    }
                    else
                    {
                        InternalKey key = new InternalKey( entry.getKey() );
                        if( key.getId().equals( idSlice ) )
                        {
                            candidats.add( entry );
                        }
                    }
                    break;
                }
            }
        }
        this.fileMetaDataLock.readLock().unlock();
        Entry<Slice,Slice> toret = null;
        int largestTime = -1;
        for( Entry<Slice,Slice> entry : candidats )
        {
            InternalKey key = new InternalKey( entry.getKey() );
            if( key.getStartTime() > largestTime && key.getStartTime() <= searchKey.getStartTime() )
            {
                toret = entry;
                largestTime = key.getStartTime();
            }
        }
        if( largestTime == -1 )
            return null;
        InternalKey key = new InternalKey( toret.getKey() );
        if( key.getValueType().getPersistentId() == ValueType.INVALID.getPersistentId() )
            return new Slice( 0 );
        else
            return toret.getValue().slice( 0, key.getValueLength() );
    }

    /**
     * 时间段查询
     */
    @Override
    public void getRangeValue( Slice idSlice, int startTime, int endTime, RangeQueryCallBack callback )
    {
        this.fileMetaDataLock.readLock().lock();
        for( int i = 4; i >=0; i-- )
        {
            FileMetaData metaData = this.files.get( (long)i );
            if( null == metaData )
                continue;
            if( metaData.getSmallest() > endTime )
                break;
            if( TimeIntervalUtil.Union( startTime, endTime, metaData.getSmallest(), metaData.getLargest() ) )
            {
                int start = Math.max( startTime, metaData.getSmallest() );
                int end = Math.min( endTime, metaData.getLargest() );
                InternalKey searchKey = new InternalKey( idSlice, start, 0, ValueType.VALUE );
                SeekingIterator<Slice,Slice> iterator = this.cache.newIterator( metaData.getNumber() );
                iterator.seek( searchKey.encode() );
                while( iterator.hasNext() )
                {
                    Entry<Slice,Slice> entry = iterator.next();
                    InternalKey key = new InternalKey( entry.getKey() );
                    if( key.getId().equals( idSlice ) && key.getStartTime() <= end && key.getValueType().getPersistentId() != ValueType.INVALID.getPersistentId() )
                    {
                        callback.onCall( entry.getValue() );
                    }
                    else
                        break;
                }
                FileBuffer buffer = this.fileBuffers.get( metaData.getNumber() );
                if( null != buffer )
                {
                    MemTableIterator bufferiterator = buffer.iterator();
                    bufferiterator.seek( searchKey.encode() );
                    while( bufferiterator.hasNext() )
                    {
                        Entry<Slice,Slice> entry = bufferiterator.next();
                        InternalKey key = new InternalKey( entry.getKey() );
                        if( key.getId().equals( idSlice ) && key.getStartTime() <= end 
                                && key.getValueType().getPersistentId() != ValueType.INVALID.getPersistentId()
                                && key.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId())
                        {
                            callback.onCall( entry.getValue() );
                        }
                        else
                            break;
                    }
                }
            }
        }
        MemTableIterator bufferiterator = this.memTable.iterator();
        InternalKey searchKey = new InternalKey( idSlice, startTime, 0, ValueType.VALUE );
        bufferiterator.seek(searchKey.encode());
        while( bufferiterator.hasNext() )
        {
            Entry<Slice,Slice> entry = bufferiterator.next();
            InternalKey key = new InternalKey( entry.getKey() );
            if( key.getId().equals( idSlice ) && key.getStartTime() <= endTime 
                    && key.getValueType().getPersistentId() != ValueType.INVALID.getPersistentId()
                    && key.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId())
            {
                callback.onCall( entry.getValue() );
            }
        }
        if( this.stableMemTable != null )
        {

            MemTableIterator stableIterator = this.stableMemTable.iterator();
            stableIterator.seek( searchKey.encode() );
            while( stableIterator.hasNext() )
            {
                Entry<Slice,Slice> entry = stableIterator.next();
                InternalKey key = new InternalKey( entry.getKey() );
                if( key.getId().equals( idSlice ) && key.getStartTime() <= endTime 
                        && key.getValueType().getPersistentId() != ValueType.INVALID.getPersistentId()
                        && key.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId())
                {
                    callback.onCall( entry.getValue() );
                }
            }
        }
        this.fileMetaDataLock.readLock().unlock();
    }

    /**
     * 写入
     */
    @Override
    public boolean set( InternalKey key, Slice value )
    {
        if( this.memTable.approximateMemoryUsage() >= 4*1024*1024 )
        {
            while( this.mergeIsHappening || this.mergeWaitingQueue.size() != 0 )
            {
                try
                {
                    Thread.currentThread().sleep(1000);
                }
                catch ( InterruptedException e )
                {
                    //FIXME
                    e.printStackTrace();
                }
            }
        }
        this.memtableLock.lock();
        this.memTable.add(key.encode(), value);
        if( !mergeIsHappening && this.memTable.approximateMemoryUsage() >= 4*1024*1024 )
        {
            MemTable temp = this.memTable;
            this.mergeWaitingQueue.offer( temp );
            this.memTable = new MemTable( TableComparator.instence() );
        }
        this.memtableLock.unlock();
        return true;
    }

    /**
     * 触发数据写入磁盘，如果需要还需要对文件进行合并
     * @param temp 需要写入磁盘的MemTable
     * @throws IOException
     */
    private void startMergeProcess( MemTable temp ) throws IOException
    {
        SeekingIterator<Slice,Slice> iterator = temp.iterator();
        this.stableMemTable = new MemTable( TableComparator.instence() );
        long countBuffer=0, countMerge = 0;
        while( iterator.hasNext() )
        {
            Entry<Slice,Slice> entry = iterator.next();
            InternalKey key = new InternalKey( entry.getKey() );
            if( key.getStartTime() < this.memTableBoundary || key.getStartTime() <= this.stLevel.getTimeBoundary() ) {
                insert2fileBuffer(key, entry.getValue());
                countBuffer++;
            }else {
                this.stableMemTable.add(entry.getKey(), entry.getValue());
                countMerge++;
            }
        }
        this.mergeProcess.merge(stableMemTable, this.files, this.fileBuffers, this.cache);
        this.memTableBoundary = Math.max(stableMemTable.getEndTime()+1,this.memTableBoundary);
        this.stableMemTable = null;
        log.debug("buffer: "+countBuffer+" merge: "+countMerge+" memTableBoundary: "+this.memTableBoundary);
    }

    /**
     * 对某个已存在的UnStableFile的插入，插入到相应的Buffer中。
     * @param key
     * @param value
     */
    private void insert2fileBuffer( InternalKey key, Slice value ) throws IOException
    {
        int insertTime = key.getStartTime();
        if( insertTime <= this.stLevel.getTimeBoundary() )
            this.stLevel.set( key, value );
        for( Long fileNumber : this.files.keySet() )
        {
            FileMetaData metaData = this.files.get( fileNumber );
            if( null == metaData )
                continue;
            if( insertTime >= metaData.getSmallest() && insertTime <= metaData.getLargest() )
            {
                FileBuffer buffer = this.fileBuffers.get( fileNumber );
                if( null == buffer )
                {
                    buffer = new FileBuffer( Filename.bufferFileName( fileNumber ), this.dbDir + "/" + Filename.bufferFileName( fileNumber ) );
                    this.fileBuffers.put( fileNumber, buffer );
                }
                buffer.add( key.encode(), value );
                break;
            }
        }
    }

    /**
     * 在系统关闭时，将MemTable中的数据写入磁盘
     */
    public void dumpMemTable2disc()
    {
        try
        {
            File tempFile = new File( this.dbDir + "/" + Filename.tempFileName( 0 ));
            if( !tempFile.exists() )
                tempFile.createNewFile();
            FileOutputStream outputStream = new FileOutputStream( tempFile );
            FileChannel channel = outputStream.getChannel();
            TableBuilder builer = new TableBuilder( new Options(), channel, TableComparator.instence() );
            SeekingIterator<Slice,Slice> iterator = this.memTable.iterator();
            while( iterator.hasNext() )
            {
                Entry<Slice,Slice> entry = iterator.next();
                builer.add( entry.getKey(), entry.getValue() );
            }
            builer.finish();
            channel.close();
            outputStream.close();
        }
        catch( IOException e )
        {
            e.printStackTrace();
        }
    }

    /**
     * 在系统关闭时，将所有UnStableFile的元数据写入磁盘
     */
    public void dumpFileMeta2disc()
    {
        try
        {
            while(this.mergeIsHappening || this.mergeWaitingQueue.size() != 0 )
                Thread.currentThread().sleep( 1000 );
            this.mergeThread.interrupt();
            while( mergeThread.isAlive() )
                this.mergeThread.interrupt();
            forceFileMetaToDisk();
        }
        catch ( InterruptedException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }

    /**
     * this method is called and blocked until merge process complete.
     */
    public synchronized void forceMemTableMerge()
    {
        if( ! mergeIsHappening )
        {
            MemTable temp = this.memTable;
            this.mergeWaitingQueue.offer( temp );
            this.memTable = new MemTable( TableComparator.instence() );
        }
        while (this.mergeIsHappening || this.mergeWaitingQueue.size() != 0)
        {
            try {
                Thread.currentThread().sleep(50);
            } catch (InterruptedException e) {
                //FIXME
                e.printStackTrace();
            }
        }
    }

    /**
     * 将所有UnStableFile的元数据写入磁盘
     */
    public void forceFileMetaToDisk()
    {
        try
        {
            LogWriter writer = Logs.createLogWriter( dbDir, false );
            VersionEdit edit = new VersionEdit();
            for( FileMetaData meta : this.files.values() )
            {
                if( null != meta )
                    edit.addFile( 0, meta );
            }
            Slice fileSlice = edit.encode();
//            fileSlice.setBytes();
            writer.addRecord(fileSlice, true);
            writer.addRecord( new Slice( "EOF!EOF!EOF!".getBytes() ), true );
            writer.close();
            File oldFile = new File(dbDir+"/unstable.meta");
            if( oldFile.exists() ) {
                if( !oldFile.delete()) throw new IOException("can not delete unstable.meta");
            }
            Path source = Paths.get(dbDir+"/unstable.new.meta");
            Files.move(source, source.resolveSibling("unstable.meta"));
        }
        catch ( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }


}
