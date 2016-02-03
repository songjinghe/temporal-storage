package org.act.dynproperty.impl;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import org.act.dynproperty.Level;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StableLevel implements Level, StableLevelAddFile
{
    private SortedMap<Integer,FileMetaData> files;
    private SortedMap<Integer,FileBuffer> fileBuffers;
    private String dbDir;
    private TableCache cache;
    private static Logger log = LoggerFactory.getLogger( StableLevel.class );
    private ReadWriteLock fileMetaLock;
    
    StableLevel( String dbDir, ReadWriteLock fileLock )
    {
        this.dbDir = dbDir;
        this.files = new TreeMap<Integer,FileMetaData>();
        this.fileBuffers = new TreeMap<Integer,FileBuffer>();
        this.cache = new TableCache( new File( dbDir ), 20, TableComparator.instence(), false, true );
        this.fileMetaLock = fileLock;
    }
    public void initfromdisc( FileMetaData metaData )
    {
        try
        {
            this.files.put( (int)metaData.getNumber(), metaData );
            String bufferName = Filename.stbufferFileName( metaData.getNumber() );
            FileBuffer buffer = new FileBuffer( bufferName, this.dbDir + "/" + bufferName );
            this.fileBuffers.put( (int)metaData.getNumber(), buffer );
        }
        catch( IOException e )
        {
            log.error( "Error happens when load bufferfile:" + metaData.getNumber() + " contents!" );
        }
    }
    int getTimeBoundary()
    {
        int lastNumber = -1;
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
    @Override
    public Slice getPointValue( Slice idSlice, int time )
    {
        InternalKey searchKey = new InternalKey( idSlice, time, 0, ValueType.VALUE );
        this.fileMetaLock.readLock().lock();
        for( Integer fileNumber : this.files.keySet() )
        {
            FileMetaData meta = this.files.get( fileNumber );
            if( null != meta && time >= meta.getSmallest() && time <= meta.getLargest() )
            {
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
        this.fileMetaLock.readLock().unlock();
        return null;
    }
    @Override
    public Slice getRangeValue( Slice idSlice, int startTime, int endTime, RangeQueryCallBack callback )
    {
        // TODO Auto-generated method stub
        return null;
    }
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
    
    private void insert2Bufferfile( InternalKey key, Slice value ) throws Exception
    {
        int insertTime = key.getStartTime();
        for( Integer fileNumber : this.files.keySet() )
        {
            FileMetaData metaData = this.files.get( fileNumber );
            if( null == metaData )
                continue;
            if( insertTime >= metaData.getSmallest() && insertTime <= metaData.getLargest() )
            {
                FileBuffer buffer = this.fileBuffers.get( fileNumber );
                if( null == buffer )
                {
                    buffer = new FileBuffer( Filename.stbufferFileName( fileNumber ), this.dbDir + "/" + Filename.stbufferFileName( fileNumber ) );
                    this.fileBuffers.put( fileNumber, buffer );
                }
                buffer.add( key.encode(), value );
                break;
            }
        }
    }
    @Override
    public void addFile( FileMetaData file )
    {
        this.files.put( (int)file.getNumber(), file );
    }
    @Override
    public long getNextFileNumber()
    {
        return this.files.size();
    }
    
    public void dumFileMeta2disc()
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
            writer.close();
        }
        catch ( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }
}
