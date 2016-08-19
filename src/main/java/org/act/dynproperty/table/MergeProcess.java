package org.act.dynproperty.table;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import org.act.dynproperty.impl.FileBuffer;
import org.act.dynproperty.impl.FileMetaData;
import org.act.dynproperty.impl.Filename;
import org.act.dynproperty.impl.InternalKey;
import org.act.dynproperty.impl.MemTable;
import org.act.dynproperty.impl.Options;
import org.act.dynproperty.impl.SeekingIterator;
import org.act.dynproperty.impl.StableLevelAddFile;
import org.act.dynproperty.impl.TableCache;
import org.act.dynproperty.util.FileChangeMonitor;
import org.act.dynproperty.util.MergingIterator;
import org.act.dynproperty.util.Slice;

/**
 * 文件合并过程
 *
 */
public class MergeProcess
{
    private String dbDir;
    private StableLevelAddFile stableLevel;
    private FileChangeMonitor fileMonitor;
    private ReadWriteLock fileMetaLock;
    
    public MergeProcess( String db, StableLevelAddFile stableLevel, ReadWriteLock fileMetaLock )
    {
        this.dbDir = db;
        this.stableLevel = stableLevel;
        this.fileMonitor = new FileChangeMonitor.Adapter();
        this.fileMetaLock = fileMetaLock;
    }
    
    /**
     * 将MemTable写入磁盘并与UnStableFile进行合并
     * @param memTable2merge 写入磁盘的MemTable
     * @param files UnStableLevel中的UnStableFile信息
     * @param fileBuffers UnStableFile对应的Buffer信息
     * @param cache 用来读取UnStableFile的缓存结构
     */
    public void merge( MemTable memTable2merge, SortedMap<Long,FileMetaData> files,
            SortedMap<Long,FileBuffer> fileBuffers, TableCache cache )
    {
        List<Long> mergeParticipants = new LinkedList<Long>();
        for( Long fileNumber : files.keySet() )
        {
            FileMetaData metaData = files.get( fileNumber );
            if( null == metaData )
                break;
            mergeParticipants.add( fileNumber );
        }
        if( mergeParticipants.size() >= 5 )
        {
            merge2StableFile( memTable2merge, files, fileBuffers, cache );
            return;
        }
        try
        {
            String targetFileName = Filename.unStableFileName( mergeParticipants.size() );
            File targetFile = new File( dbDir + "/" + targetFileName );
            if( targetFile.exists() )
                targetFile.delete();
            targetFile.createNewFile();
            FileOutputStream targetStream = new FileOutputStream( targetFile );
            FileChannel targetChannel = targetStream.getChannel();
            TableBuilder builder = new TableBuilder( new Options(), targetChannel, TableComparator.instence() );
            
            List<SeekingIterator<Slice,Slice>> mergeIterators = new LinkedList<SeekingIterator<Slice,Slice>>();
            List<Closeable> channel2close = new LinkedList<Closeable>();
            List<File> files2delete = new LinkedList<File>();
            List<Long> table2evict = new LinkedList<Long>();
            
            mergeIterators.add( memTable2merge.iterator() );
            channel2close.add( targetStream );
            channel2close.add( targetChannel );
            for( Long fileNumber : mergeParticipants )
            {
                FileMetaData metaData = files.get( fileNumber );
                File mergeFile = new File( dbDir + "/" + Filename.unStableFileName( metaData.getNumber() ) );
//                FileInputStream inputStream = new FileInputStream( mergeFile );
//                FileChannel channel = inputStream.getChannel();
//                channel2close.add( channel );
//                channel2close.add( inputStream );
                table2evict.add( (long)fileNumber );
                files2delete.add( mergeFile );
                //Table table = new FileChannelTable( Filename.unStableFileName( metaData.getNumber() ), channel, TableComparator.instence(), false );
                
                FileBuffer filebuffer = fileBuffers.get( fileNumber );
                Table table = cache.newTable( fileNumber );
                SeekingIterator<Slice,Slice> mergeIterator;
                if( null != filebuffer )
                {
//                    mergeIterator = new BufferFileAndTableIterator( filebuffer.iterator(), 
//                        table.iterator(), TableComparator.instence() );
//                    channel2close.add( filebuffer );
                    mergeIterator = new BufferFileAndTableIterator( filebuffer.iterator(), 
                            table.iterator(), TableComparator.instence() );
                    channel2close.add( filebuffer );
                    files2delete.add( new File( dbDir + "/" + Filename.bufferFileName( fileNumber ) ) );
                }
                else
                    mergeIterator = table.iterator();
                channel2close.add( table );
                table2evict.add( fileNumber );
                mergeIterators.add( mergeIterator );
            }
            MergingIterator buildIterator = new MergingIterator( mergeIterators, TableComparator.instence() );
            int smallest = Integer.MAX_VALUE;
            int largest = -1;
            int count = 0;
            while( buildIterator.hasNext() )
            {
                Entry<Slice,Slice> entry = buildIterator.next();
                InternalKey key = new InternalKey( entry.getKey() );
                if( key.getStartTime() < smallest )
                    smallest = key.getStartTime();
                if( key.getStartTime() > largest )
                    largest = key.getStartTime();
                builder.add( entry.getKey(), entry.getValue() );
                count++;
            }
            builder.finish();
            this.fileMetaLock.writeLock().lock();
            {
                FileMetaData targetMetaData = new FileMetaData( mergeParticipants.size(), targetChannel.size(), smallest, largest );
                files.put( targetMetaData.getNumber(), targetMetaData );
                String bufferFilename = Filename.bufferFileName( mergeParticipants.size() );
                //fileBuffers.put( mergeParticipants.size(), new FileBuffer( bufferFilename, dbDir + "/" + bufferFilename ) );
                fileMonitor.addFile( 0, targetMetaData );
                for( Closeable c : channel2close )
                {
                    c.close();
                }
                for( Long num : table2evict )
                    cache.evict( num );
                //delete obsolet files
                for( Long fileNumber : mergeParticipants )
                {
                    FileMetaData metaData = files.get( fileNumber );
                    File mergeFile = new File( dbDir + "/" + Filename.unStableFileName( metaData.getNumber() ) );
                    files.put( fileNumber, null );
                    fileBuffers.put( fileNumber, null );
                    fileMonitor.deleteFile( 0, metaData );
                }
                for( File f : files2delete )
                {
                    Files.delete( f.toPath() );
                }
            }
            this.fileMetaLock.writeLock().unlock();
        }
        catch( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }

    /**
     * 将UnStableFile合并为StableFile的过程
     */
    private void merge2StableFile( MemTable memTable2merge, SortedMap<Long,FileMetaData> files,
            SortedMap<Long,FileBuffer> fileBuffers, TableCache cache )
    {
        List<Long> mergeParticipants = new LinkedList<Long>();
        for( Long fileNumber : files.keySet() )
        {
            FileMetaData metaData = files.get( fileNumber );
            if( null == metaData )
                break;
            mergeParticipants.add( fileNumber );
        }
        if( mergeParticipants.size() < 5 )
        {
            merge( memTable2merge, files, fileBuffers, cache );
            return;
        }
        try
        {
            String targetFileName = Filename.stableFileName( this.stableLevel.getNextFileNumber() );
            File targetFile = new File( dbDir + "/" + targetFileName );
            if( targetFile.exists() )
                targetFile.delete();
            targetFile.createNewFile();
            FileOutputStream targetStream = new FileOutputStream( targetFile );
            FileChannel targetChannel = targetStream.getChannel();
            TableBuilder builder = new TableBuilder( new Options(), targetChannel, TableComparator.instence() );
            
            List<SeekingIterator<Slice,Slice>> mergeIterators = new LinkedList<SeekingIterator<Slice,Slice>>();
            List<Closeable> channel2close = new LinkedList<Closeable>();
            List<File> files2delete = new LinkedList<File>();
            List<Long> table2evict = new LinkedList<Long>();
            
            mergeIterators.add( memTable2merge.iterator() );
            SeekingIterator<Slice,Slice> lastFile = this.stableLevel.getlastFileIterator();
            if( null != lastFile )
                mergeIterators.add( this.stableLevel.getlastFileIterator() );
            channel2close.add( targetStream );
            channel2close.add( targetChannel );
            for( Long fileNumber : mergeParticipants )
            {
                FileMetaData metaData = files.get( fileNumber );
                File mergeFile = new File( dbDir + "/" + Filename.unStableFileName( metaData.getNumber() ) );
//                FileInputStream inputStream = new FileInputStream( mergeFile );
//                FileChannel channel = inputStream.getChannel();
//                channel2close.add( channel );
//                channel2close.add( inputStream );
                table2evict.add( (long)fileNumber );
                files2delete.add( mergeFile );
                //Table table = new FileChannelTable( Filename.unStableFileName( metaData.getNumber() ), channel, TableComparator.instence(), false );
                
                FileBuffer filebuffer = fileBuffers.get( fileNumber );
                Table table = cache.newTable( fileNumber );
                SeekingIterator<Slice,Slice> mergeIterator;
                if( null != filebuffer )
                {
//                    mergeIterator = new BufferFileAndTableIterator( filebuffer.iterator(), 
//                        table.iterator(), TableComparator.instence() );
//                    channel2close.add( filebuffer );
                    mergeIterator = new BufferFileAndTableIterator( filebuffer.iterator(), 
                            table.iterator(), TableComparator.instence() );
                    channel2close.add( filebuffer );
                    files2delete.add( new File( dbDir + "/" + Filename.bufferFileName( fileNumber ) ) );
                }
                else
                    mergeIterator = table.iterator();
                channel2close.add( table );
                table2evict.add( fileNumber );
                mergeIterators.add( mergeIterator );
            }
            MergingIterator buildIterator = new MergingIterator( mergeIterators, TableComparator.instence() );
            //int smallest = Integer.MAX_VALUE;
            int largest = -1;
            int count = 0;
            while( buildIterator.hasNext() )
            {
                Entry<Slice,Slice> entry = buildIterator.next();
                InternalKey key = new InternalKey( entry.getKey() );
//                if( key.getStartTime() < smallest )
//                    smallest = key.getStartTime();
                if( key.getStartTime() > largest )
                    largest = key.getStartTime();
                builder.add( entry.getKey(), entry.getValue() );
                count++;
            }
            builder.finish();
            FileMetaData targetMetaData = new FileMetaData( this.stableLevel.getNextFileNumber(), targetChannel.size(), this.stableLevel.getlastBoundary(), largest );
            this.stableLevel.addFile( targetMetaData );
            this.fileMonitor.addFile( 1, targetMetaData );
            //files.put( mergeParticipants.size(), targetMetaData );
            //String bufferFilename = Filename.bufferFileName( mergeParticipants.size() );
            //fileBuffers.put( mergeParticipants.size(), new FileBuffer( bufferFilename, dbDir + "/" + bufferFilename ) );
            this.fileMetaLock.writeLock().lock();
            {
                for( Closeable c : channel2close )
                {
                    c.close();
                }
                //delete obsolet files
                for( Long num : table2evict )
                    cache.evict( num );
                for( Long fileNumber : mergeParticipants )
                {
                    FileMetaData metaData = files.get( fileNumber );
                    File mergeFile = new File( dbDir + "/" + Filename.unStableFileName( metaData.getNumber() ) );
                    files.put( fileNumber, null );
                    fileBuffers.put( fileNumber, null );
                    this.fileMonitor.deleteFile( 0, metaData );
                }
                for( File f : files2delete )
                {
                    Files.delete( f.toPath() );
                }
            }
            this.fileMetaLock.writeLock().unlock();
        }
        catch( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }
}
