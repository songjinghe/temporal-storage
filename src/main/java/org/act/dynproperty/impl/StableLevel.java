package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import org.act.dynproperty.Level;
import org.act.dynproperty.impl.MemTable.MemTableIterator;
import org.act.dynproperty.table.BufferFileAndTableIterator;
import org.act.dynproperty.table.Table;
import org.act.dynproperty.table.TableBuilder;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.table.TableIterator;
import org.act.dynproperty.util.MergingIterator;
import org.act.dynproperty.util.Slice;
import org.act.dynproperty.util.TableLatestValueIterator;
import org.act.dynproperty.util.TimeIntervalUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StableLevel中存储着所有UnStableFile的相关元信息，并且是对其进行查询和写入的入口
 *
 */
public class StableLevel implements Level, StableLevelAddFile
{
	/**
	 * 所有的StableFile的元信息
	 */
    private SortedMap<Long,FileMetaData> files;
    /**
     * 所有StableFile对应的Buffer
     */
    private SortedMap<Long,FileBuffer> fileBuffers;
    /**
     * 存储目录
     */
    private String dbDir;
    /**
     * 对StableFile进行的缓存
     */
    private TableCache cache;
    private static Logger log = LoggerFactory.getLogger( StableLevel.class );
    /**
     * 修改和读取StableFile元信息时的读写锁
     */
    private ReadWriteLock fileMetaLock;
    private final int bufferMergeboundary = 2;
    
    StableLevel( String dbDir, ReadWriteLock fileLock )
    {
        this.dbDir = dbDir;
        this.files = new TreeMap<Long,FileMetaData>();
        this.fileBuffers = new TreeMap<Long,FileBuffer>();
        this.cache = new TableCache( new File( dbDir ), 20, TableComparator.instence(), false, true );
        this.fileMetaLock = fileLock;
    }
    
    /**
     * 回复相关文件元信息，判断其是否有Buffer存在。
     */
    public void initfromdisc( FileMetaData metaData )
    {
        recoverIfNeeded(metaData);
        loadBufferIfExist(metaData);
    }

    private void loadBufferIfExist(FileMetaData metaData) {
        try
        {
            String bufferName = Filename.stbufferFileName(metaData.getNumber());
            File bufferfile = new File(this.dbDir + "/" + bufferName);
            if (bufferfile.exists()) {
                FileBuffer buffer = new FileBuffer(bufferName, this.dbDir + "/" + bufferName);
                this.fileBuffers.put(metaData.getNumber(), buffer);
            } else
                this.fileBuffers.put(metaData.getNumber(), null);
        }
        catch( IOException e )
        {
            log.error( "Error happens when load bufferfile:" + metaData.getNumber() + " contents!" );
            throw new RuntimeException(e);
        }
    }

    private void recoverIfNeeded(FileMetaData metaData) {
        File file = new File(this.dbDir + "/" + Filename.stableFileName(metaData.getNumber()));
        if(file.exists()){
            if(file.isDirectory()) {
                throw new RuntimeException("found data file "+file.getName()+" as dir!");
            }else{
                return;
            }
        }else{ // not exist, search for 000007.dbtmp and rename it.
            File tmpFile = new File(this.dbDir+'/'+Filename.tempFileName(7));
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
     * 返回StableLevel存储的数据的最晚有效时间
     */
    long getTimeBoundary()
    {
        long lastNumber = -1l;
        try
        {
            lastNumber = this.files.lastKey();
        }
        catch( NoSuchElementException e )
        {
            return lastNumber;
        }
        return this.files.get( lastNumber ).getLargest();
    }
    /**
     * 进行时间点查询
     */
    @Override
    public Slice getPointValue( Slice idSlice, int time )
    {
        InternalKey searchKey = new InternalKey( idSlice, time, 0, ValueType.VALUE );
        this.fileMetaLock.readLock().lock();
        boolean found = false;
        for( long fileNumber : this.files.keySet() )
        {
            FileMetaData meta = this.files.get( fileNumber );
            if( null != meta && time >= meta.getSmallest() && time <= meta.getLargest() )
            {
            	found = true;
                SeekingIterator<Slice,Slice> iterator = this.cache.newIterator( meta );
                iterator.seek( searchKey.encode() );
                Entry<Slice,Slice> entry = iterator.next();
                FileBuffer buffer = this.fileBuffers.get( fileNumber );
                if( null != buffer )
                {
                    SeekingIterator<Slice,Slice> bufferIterator = this.fileBuffers.get( fileNumber ).iterator();
                    bufferIterator.seek( searchKey.encode() );
                    Entry<Slice,Slice> bufferEntry = bufferIterator.next();
                    InternalKey bufferkey = new InternalKey( bufferEntry.getKey() );
                    InternalKey key = new InternalKey( entry.getKey() );
                    if( key.getId().equals( idSlice ) )
                    {
                        if( bufferkey.getId().equals( idSlice ) )
                        {
                            if( key.getStartTime() == bufferkey.getStartTime() )
                            {
                                if( bufferkey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId() )
                                {
                                    this.fileMetaLock.readLock().unlock();
                                    return bufferEntry.getValue().slice( 0, bufferkey.getValueLength() );
                                }
                            }
                            else if( bufferkey.getStartTime() <= time && bufferkey.getStartTime() > key.getStartTime() && bufferkey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId()
                                    && bufferkey.getValueType().getPersistentId() != ValueType.INVALID.getPersistentId())
                            {
                                this.fileMetaLock.readLock().unlock();
                                return bufferEntry.getValue().slice( 0, bufferkey.getValueLength() );
                            }
                        }
                        this.fileMetaLock.readLock().unlock();
                        return entry.getValue().slice( 0, key.getValueLength() );
                    }
                }
                else
                {
                    InternalKey key = new InternalKey( entry.getKey() );
                    if( key.getId().equals( idSlice ) )
                    {
                        this.fileMetaLock.readLock().unlock();
                        return entry.getValue().slice( 0, key.getValueLength() ); 
                    }
                    this.fileMetaLock.readLock().unlock();
                    return null;
                }
            }
        }
        if(!found){
        	long fileNumber = this.files.lastKey();
        	FileMetaData meta = this.files.get(fileNumber);
        	SeekingIterator<Slice,Slice> iterator = this.cache.newIterator( meta );
            iterator.seek( searchKey.encode() );
            Entry<Slice,Slice> entry = iterator.next();
            FileBuffer buffer = this.fileBuffers.get( fileNumber );
            if( null != buffer )
            {
                SeekingIterator<Slice,Slice> bufferIterator = this.fileBuffers.get( fileNumber ).iterator();
                bufferIterator.seek( searchKey.encode() );
                Entry<Slice,Slice> bufferEntry = bufferIterator.next();
                InternalKey bufferkey = new InternalKey( bufferEntry.getKey() );
                InternalKey key = new InternalKey( entry.getKey() );
                if( key.getId().equals( idSlice ) )
                {
                    if( bufferkey.getId().equals( idSlice ) )
                    {
                        if( key.getStartTime() == bufferkey.getStartTime() )
                        {
                            if( bufferkey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId() )
                            {
                                this.fileMetaLock.readLock().unlock();
                                return bufferEntry.getValue().slice( 0, bufferkey.getValueLength() );
                            }
                        }
                        else if( bufferkey.getStartTime() <= time && bufferkey.getStartTime() > key.getStartTime() && bufferkey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId()
                                && bufferkey.getValueType().getPersistentId() != ValueType.INVALID.getPersistentId())
                        {
                            this.fileMetaLock.readLock().unlock();
                            return bufferEntry.getValue().slice( 0, bufferkey.getValueLength() );
                        }
                    }
                    this.fileMetaLock.readLock().unlock();
                    return entry.getValue().slice( 0, key.getValueLength() );
                }
            }
            else
            {
                InternalKey key = new InternalKey( entry.getKey() );
                if( key.getId().equals( idSlice ) )
                {
                    this.fileMetaLock.readLock().unlock();
                    return entry.getValue().slice( 0, key.getValueLength() ); 
                }
                this.fileMetaLock.readLock().unlock();
                return null;
            }
        }
        this.fileMetaLock.readLock().unlock();
        return null;
    }
    /**
     * 进行时间段查询
     */
    @Override
    public void getRangeValue( Slice idSlice, int startTime, int endTime, RangeQueryCallBack callback )
    {
        this.fileMetaLock.readLock().lock();
        for( FileMetaData metaData : this.files.values() )
        {
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
                    bufferiterator.seek(searchKey.encode());
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
        this.fileMetaLock.readLock().unlock();
    }
    /**
     * 进行写入
     */
    @Override
    public boolean set( InternalKey key, Slice value )
    {
        try
        {
            insert2Bufferfile( key, value );
        }
        catch( Throwable t )
        {
            return false;
        }
        return true;
    }
    
    /**
     * 针对某个Buffer的插入操作， 如果不存在则新建一个Buffer
     */
    private void insert2Bufferfile( InternalKey key, Slice value ) throws Exception
    {
        int insertTime = key.getStartTime();
        this.fileMetaLock.writeLock().lock();
        for( long fileNumber : this.files.keySet() )
        {
            FileMetaData metaData = this.files.get( fileNumber );
            if( null == metaData )
                continue;
            if( insertTime >= metaData.getSmallest() && insertTime <= metaData.getLargest() )
            {
                FileBuffer buffer = this.fileBuffers.get(fileNumber);
                if( null == buffer )
                {
                    buffer = new FileBuffer( Filename.stbufferFileName( fileNumber ), this.dbDir + "/" + Filename.stbufferFileName( fileNumber ) );
                    this.fileBuffers.put( fileNumber, buffer );
                }
                buffer.add( key.encode(), value );
                if(buffer.size()>1024*1024*10) {
                    mergeBufferToFile( fileNumber );
                }
                break;
            }
        }
        this.fileMetaLock.writeLock().unlock();
    }
    
    /**
     * 当有新的StableFile生成的时候，通过这个方法将其信息加入StableLevel
     */
    @Override
    public void addFile( FileMetaData file ) throws IOException
    {
        this.fileMetaLock.writeLock().lock();
        if( this.files.size() >= bufferMergeboundary )
        {
            long fileNumber = (long) this.files.size() - bufferMergeboundary;
            mergeBufferToFile(fileNumber);
        }
        this.files.put(file.getNumber(), file);
        this.fileMetaLock.writeLock().unlock();
    }
    
    /**
     * 返回最新的StableFile的编号
     */
    @Override
    public long getNextFileNumber()
    {
        return this.files.size();
    }
    
    /**
     * 在系统关闭的时候，将所有的文件元信息写入磁盘
     */
    public void dumpFileMeta2disc()
    {
        try
        {
            LogWriter writer = Logs.createLogWriter( dbDir, true );
            VersionEdit edit = new VersionEdit();
            for( FileMetaData meta : this.files.values() )
            {
                if( null != meta )
                    edit.addFile( 0, meta );
            }
            writer.addRecord( edit.encode(), true );
            writer.addRecord( new Slice( "EOF!EOF!EOF!".getBytes() ), true );
            writer.close();
            File oldFile = new File(dbDir+"/stable.meta");
            if( oldFile.exists() ) {
                if( !oldFile.delete()) throw new IOException("can not delete stable.meta");
            }
            Path source = Paths.get(dbDir + "/stable.new.meta");
            Files.move(source, source.resolveSibling("stable.meta"));
        }
        catch ( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }
    
    @Override
    public SeekingIterator<Slice,Slice> getlastFileIterator()
    {
        if( this.files.size() > 0 )
        {
            SeekingIterator<Slice,Slice> table = this.cache.newIterator( this.files.get( (long)this.files.size()-1 ).getNumber() );
            if( this.fileBuffers.get( (long)this.files.size()-1 ) != null )
            {
                MemTableIterator buffer = this.fileBuffers.get( (long)this.files.size()-1 ).iterator();
                table = new BufferFileAndTableIterator( buffer, table, TableComparator.instence() );
            }
            return new TableLatestValueIterator( table );
        }
        return null;
    }
    @Override
    public int getlastBoundary()
    {
        if(this.files.size() == 0 )
            return 0;
        else
            return this.files.get( (long)this.files.size()-1 ).getLargest()+1;
    }

    /**
     * this method merge (but not upgrade levels) buffer file to its
     * responsible stable file.
     * this method is ONLY used for stable files.
     * if no buffer then nothing happened.
     * note: meta data is not update, therefore the size of file is not update.
     * @throws IOException
     */
    private void mergeBufferToFile(long number) throws IOException {
        FileBuffer buffer = this.fileBuffers.get( number );
        if( null != buffer )
        {
            Table table = this.cache.newTable( number );
            String tempfilename = Filename.tempFileName( 7 );
            File tempFile = new File(this.dbDir + "/" + tempfilename );
            if( !tempFile.exists() )
                tempFile.createNewFile();
            FileOutputStream stream = new FileOutputStream( tempFile );
            FileChannel channel = stream.getChannel();
            TableBuilder builder = new TableBuilder( new Options(), channel, TableComparator.instence() );
            List<SeekingIterator<Slice,Slice>> iterators = new ArrayList<SeekingIterator<Slice,Slice>>(2);
            SeekingIterator<Slice,Slice> iterator = new BufferFileAndTableIterator( buffer.iterator(), table.iterator(), TableComparator.instence() );
            iterators.add( iterator );
            MergingIterator mergeIterator = new MergingIterator( iterators, TableComparator.instence() );
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
            File originFile = new File( this.dbDir + "/" + Filename.stableFileName(number));
            Files.delete( originFile.toPath() );
            buffer.close();
            Files.delete(new File(this.dbDir + "/" + Filename.stbufferFileName( number ) ).toPath());
            this.fileBuffers.put( number, null);
            tempFile.renameTo(new File(this.dbDir + "/" + Filename.stableFileName( number ) ) );
        }
    }

}
