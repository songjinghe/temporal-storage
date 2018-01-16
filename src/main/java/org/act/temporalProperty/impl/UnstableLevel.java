package org.act.temporalProperty.impl;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import org.act.temporalProperty.Level;
import org.act.temporalProperty.impl.MemTable.MemTableIterator;
import org.act.temporalProperty.table.*;
import org.act.temporalProperty.util.MergingIterator;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.TimeIntervalUtil;
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
        cache = new TableCache( new File( dbDir ), 5, TableComparator.instance(), false, false );
        this.memtableLock = new ReentrantLock( true );
        this.fileMetaDataLock = fileLock;
        this.stLevel = stLevel;
        this.mergeThread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                Thread.currentThread().setName("TemporalPropStore-"+(dbDir.endsWith("temporal.node.properties")?"Node":"Rel"));
                while(true)
                {
                    try
                    {
                        MemTable temp = mergeWaitingQueue.take();
                        mergeIsHappening = true;
                        if(!temp.isEmpty()) {
                            UnstableLevel.this.startMergeProcess(temp);
                        }else{
//                            log.debug("empty memtable");
                        }
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
    @Override
    public void initFromDisk(FileMetaData metaData )
    {
        recoverIfNeeded(metaData);
        loadBufferIfExist(metaData);
    }

    private void loadBufferIfExist(FileMetaData metaData) {
        try{
            this.files.put(metaData.getNumber(), metaData);
            String bufferName = Filename.bufferFileName(metaData.getNumber());
            File bufferfile = new File(this.dbDir + "/" + bufferName );
            if( bufferfile.exists() )
            {
                FileBuffer buffer = new FileBuffer( bufferName, this.dbDir + "/" + bufferName );
                this.fileBuffers.put(metaData.getNumber(), buffer);
            }
            else
                this.fileBuffers.put( metaData.getNumber(), null );
        }
        catch( IOException e )
        {
            log.error( "Error happens when load bufferfile:" + metaData.getNumber() + " contents!" );
            throw new RuntimeException(e);
        }
    }

    private void recoverIfNeeded(FileMetaData metaData) {
        File file = new File(this.dbDir + "/" + Filename.unStableFileName(metaData.getNumber()));
        if(file.exists()){
            if(file.isDirectory()) {
                throw new RuntimeException("found data file "+file.getName()+" as dir!");
            }else{
                return;
            }
        }else{ // not exist, search for 000006.dbtmp and rename it.
            File tmpFile = new File(this.dbDir+'/'+Filename.tempFileName(6));
            if(tmpFile.exists()){
                if(!tmpFile.renameTo(file)){
                    throw new RuntimeException("Recovery: can not rename " + tmpFile.getName() + " file!");
                }else{
                    return;
                }
            }else{
                throw new RuntimeException("data file "+file.getName()+" not found, db is damaged.");
            }
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
            if( tempFile.exists() && (tempFile.length() >= Footer.ENCODED_LENGTH))
            {
                FileInputStream inputStream = new FileInputStream(tempFile);
                FileChannel channel = inputStream.getChannel();
                Table table;
                try {
                    table = new FileChannelTable(tempFileName, channel, TableComparator.instance(), false);
                }catch (IllegalArgumentException e){
                    throw new RuntimeException(tempFileName+" file size larger than "+Integer.MAX_VALUE+" bytes. Should not happen.",e);
                }
                TableIterator iterator = table.iterator();
                if( iterator.hasNext() )
                {
                    this.memTable = new MemTable(TableComparator.instance());
                    while (iterator.hasNext()) {
                        Entry<Slice, Slice> entry = iterator.next();
                        this.memTable.add(entry.getKey(), entry.getValue());
                    }
                    this.memTableBoundary = this.memTable.getStartTime();
                }
                else
                {
                    this.createNewEmptyMemTable();
                }
                channel.close();
                inputStream.close();
                Files.delete(tempFile.toPath());
            }
            else
            {
                this.createNewEmptyMemTable();
            }
        }
        catch( IOException e )
        {
            log.error( "Restore MemTable Failed!", e );
        }
    }

    private void createNewEmptyMemTable() {
        this.memTable = new MemTable(TableComparator.instance());
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
        List<Entry<Slice,Slice>> candidates = new LinkedList<Entry<Slice,Slice>>();
        {
            try
            {
                SeekingIterator<Slice,Slice> iterator = this.memTable.iterator();
                iterator.seek( searchKey.encode() );
                Entry<Slice,Slice> entry = iterator.next();
                InternalKey answerKey = new InternalKey( entry.getKey() );
                if( answerKey.getId().equals( idSlice ) && answerKey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId()  )
                {
                    candidates.add( entry );
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
                        candidates.add( entry );
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
                                    candidates.add( bufferEntry );
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
                                candidates.add( entry );
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
                                        candidates.add( bufferEntry );
                                }
                                else
                                {
                                    if( key.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId() )
                                        candidates.add( bufferEntry );
                                    candidates.add( entry );
                                }
                            }
                            else
                                candidates.add( entry );
                        }
                    }
                    else
                    {
                        InternalKey key = new InternalKey( entry.getKey() );
                        if( key.getId().equals( idSlice ) )
                        {
                            candidates.add( entry );
                        }
                    }
                    break;
                }
            }
        }
        this.fileMetaDataLock.readLock().unlock();
        Entry<Slice,Slice> toret = null;
        int largestTime = -1;
        for( Entry<Slice,Slice> entry : candidates )
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
                        callback.onCall(key.getStartTime(), entry.getValue());
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
                            callback.onCall(key.getStartTime(), entry.getValue());
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
                callback.onCall(key.getStartTime(), entry.getValue());
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
                    callback.onCall(key.getStartTime(), entry.getValue());
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
                    Thread.sleep(200);
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
            this.memTable = new MemTable( TableComparator.instance() );
        }
        this.memtableLock.unlock();
        return true;
    }

    void debugMemtable(String mark, MemTable t){
        MemTableIterator i = t.iterator();
        long size = 0;
        log.debug("{}-{} {}", t.getStartTime(), t.getEndTime());
        log.debug("{}", t.approximateMemoryUsage());
//        while(i.hasNext()){i.next(); size++;}
//        log.info(mark+" {} size:{}", t, size);
    }

    /**
     * 触发数据写入磁盘，如果需要还需要对文件进行合并
     * @param temp 需要写入磁盘的MemTable
     * @throws IOException
     */
    private void startMergeProcess( MemTable temp ) throws IOException
    {
        SeekingIterator<Slice,Slice> iterator = temp.iterator();
        this.stableMemTable = new MemTable( TableComparator.instance() );
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
        if(!stableMemTable.isEmpty()){
            this.mergeProcess.merge(stableMemTable, this.files, this.fileBuffers, this.cache);
            this.memTableBoundary = Math.max(stableMemTable.getEndTime()+1,this.memTableBoundary);
        }
        this.stableMemTable = null;
//        log.debug("buffer: "+countBuffer+" merge: "+countMerge+" memTableBoundary: "+this.memTableBoundary);
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
                if(buffer.size()>1024*1024*10) {
                    mergeBufferToFile( fileNumber );
                }
                break;
            }
        }
    }

    /**
     * this method merge (but not upgrade levels) buffer file to its
     * responsible unstable file. if no buffer then nothing happened.
     * @param number
     * @throws IOException
     */
    private void mergeBufferToFile(Long number) throws IOException {
        FileBuffer buffer = this.fileBuffers.get( number );
        if( null != buffer )
        {
            Table table = this.cache.newTable( number );
            String tempfilename = Filename.tempFileName( 6 );
            File tempFile = new File(this.dbDir + "/" + tempfilename );
            if( !tempFile.exists() )
                tempFile.createNewFile();
            FileOutputStream stream = new FileOutputStream( tempFile );
            FileChannel channel = stream.getChannel();
            TableBuilder builder = new TableBuilder( new Options(), channel, TableComparator.instance() );
            List<SeekingIterator<Slice,Slice>> iterators = new ArrayList<SeekingIterator<Slice,Slice>>(2);
            SeekingIterator<Slice,Slice> iterator = new BufferFileAndTableIterator( buffer.iterator(), table.iterator(), TableComparator.instance() );
            iterators.add( iterator );
            MergingIterator mergeIterator = new MergingIterator( iterators, TableComparator.instance() );
            while( mergeIterator.hasNext() )
            {
                Entry<Slice,Slice> entry = mergeIterator.next();
                builder.add( entry.getKey(), entry.getValue() );
            }
            builder.finish();
            channel.close();
            stream.close();
            table.close();
            this.cache.evict( number );
            File originFile = new File( this.dbDir + "/" + Filename.unStableFileName( number ));
            Files.delete( originFile.toPath() );
            buffer.close();
            Files.delete( new File( this.dbDir + "/" + Filename.bufferFileName( number ) ).toPath() );
            this.fileBuffers.put( number, null );
            tempFile.renameTo( new File( this.dbDir + "/" + Filename.unStableFileName( number ) ) );
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
            TableBuilder builer = new TableBuilder( new Options(), channel, TableComparator.instance() );
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
            waitUntilCurrentMergeComplete();
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
     * this method is called from [main] thread and will block until merge process complete.
     * Note: this method can only guarantee that data written in memtable before the time this
     * method is called is written into disk. SET operation are blocked when this method is running.
     */
    public synchronized void forceMemTableMerge()
    {
        try {
            this.memtableLock.lock();
            MemTable temp = this.memTable;
            this.mergeWaitingQueue.offer(temp);
            this.memTable = new MemTable(TableComparator.instance());
            waitUntilCurrentMergeComplete();
        }catch (InterruptedException e){
            //FIXME
            log.warn("Force merge interrupted. Merge operation may failed.");
            e.printStackTrace();
        }finally {
            this.memtableLock.unlock();
        }
    }

    private void waitUntilCurrentMergeComplete() throws InterruptedException{
        while (this.mergeIsHappening || this.mergeWaitingQueue.size() != 0)
        {
            Thread.sleep(50);
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
