package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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
import org.act.dynproperty.table.FileChannelTable;
import org.act.dynproperty.table.MergeProcess;
import org.act.dynproperty.table.Table;
import org.act.dynproperty.table.TableBuilder;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.table.TableIterator;
import org.act.dynproperty.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UnstableLevel implements Level
{
    private SortedMap<Long,FileMetaData> files;
    private SortedMap<Long,FileBuffer> fileBuffers;
    private String dbDir;
    private MemTable memTable;
    private MemTable stableMemTable;
    private int memTableBoundary;
    private MergeProcess mergeProcess;
    private static Logger log = LoggerFactory.getLogger( UnstableLevel.class );
    private TableCache cache;
    private ReentrantLock memtableLock;
    private BlockingQueue<MemTable> mergeWaitingQueue = new LinkedBlockingQueue<MemTable>();
    private Thread mergeThread;
    private volatile boolean mergeIsHappening = false;
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
    
    public void initfromdisc( FileMetaData metaData )
    {
        try
        {
            this.files.put( metaData.getNumber(), metaData );
            String bufferName = Filename.bufferFileName( metaData.getNumber() );
            FileBuffer buffer = new FileBuffer( bufferName, this.dbDir + "/" + bufferName );
            this.fileBuffers.put( metaData.getNumber(), buffer );
        }
        catch( IOException e )
        {
            log.error( "Error happens when load bufferfile:" + metaData.getNumber() + " contents!" );
        }
    }
    
    public void restoreMemTable()
    {
        try
        {
            String tempFileName = Filename.tempFileName( 0 );
            File tempFile = new File( this.dbDir + "/" + tempFileName );
            if( !tempFile.exists() )
            {
                this.memTable = new MemTable( TableComparator.instence() );
                this.memTableBoundary = 0;
                for( Long number : this.files.keySet() )
                {
                    if( this.files.get( number ) != null )
                    {
                        this.memTableBoundary = this.files.get( number ).getLargest()+1;
                        break;
                    }
                }
                return;
            }
            FileInputStream inputStream = new FileInputStream( tempFile );
            FileChannel channel = inputStream.getChannel();
            Table table = new FileChannelTable( tempFileName, channel, TableComparator.instence(), false );
            TableIterator iterator = table.iterator();
            this.memTable = new MemTable( TableComparator.instence() );
            while( iterator.hasNext() )
            {
                Entry<Slice,Slice> entry = iterator.next(); 
                this.memTable.add( entry.getKey(), entry.getValue() );
            }
            this.memTableBoundary = this.memTable.getStartTime();
            inputStream.close();
            channel.close();
            Files.delete( tempFile.toPath() );
        }
        catch( IOException e )
        {
            log.error( "Restore MemTable Failed!" );
        }
    }

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

    @Override
    public Slice getRangeValue( Slice idSlice, int startTime, int endTime, RangeQueryCallBack callback )
    {
        // TODO Auto-generated method stub
        return null;
    }

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
        this.memTable.add( key.encode(), value );
        if( !mergeIsHappening && this.memTable.approximateMemoryUsage() >= 4*1024*1024 )
        {
            MemTable temp = this.memTable;
            this.mergeWaitingQueue.offer( temp );
            this.memTable = new MemTable( TableComparator.instence() );
        }
        this.memtableLock.unlock();
        return true;
    }

    private void startMergeProcess( MemTable temp ) throws IOException
    {
        SeekingIterator<Slice,Slice> iterator = temp.iterator();
        this.stableMemTable = new MemTable( TableComparator.instence() );
        while( iterator.hasNext() )
        {
            Entry<Slice,Slice> entry = iterator.next();
            InternalKey key = new InternalKey( entry.getKey() );
            if( key.getStartTime() < this.memTableBoundary || key.getStartTime() <= this.stLevel.getTimeBoundary() )
                insert2fileBuffer( key, entry.getValue() );
            else
                this.stableMemTable.add( entry.getKey(), entry.getValue() );
        }
        this.mergeProcess.merge( stableMemTable, this.files, this.fileBuffers, this.cache );
        this.memTableBoundary = stableMemTable.getEndTime()+1;
        this.stableMemTable = null;
    }

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

    public void dumpFileMeta2disc()
    {
        try
        {
            while(this.mergeIsHappening || this.mergeWaitingQueue.size() != 0 )
                Thread.currentThread().sleep( 1000 );
            this.mergeThread.interrupt();
            while( mergeThread.isAlive() )
                this.mergeThread.interrupt();
            LogWriter writer = Logs.createLogWriter( dbDir, false );
            VersionEdit edit = new VersionEdit();
            for( FileMetaData meta : this.files.values() )
            {
                if( null != meta )
                    edit.addFile( 0, meta );
            }
            writer.addRecord( edit.encode(), true );
            writer.close();
        }
        catch ( IOException | InterruptedException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }
}
