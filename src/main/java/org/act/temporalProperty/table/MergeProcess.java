package org.act.temporalProperty.table;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.util.MergingIterator;
import org.act.temporalProperty.util.Slice;

/**
 * 文件合并过程
 *
 */
public class MergeProcess
{
    private String dbDir;
    private StableLevel stableLevel;
    private ReadWriteLock fileMetaLock;
    private UnstableLevel unStableLevel;



    public MergeProcess( String db, StableLevel stableLevel, ReadWriteLock fileMetaLock )
    {
        this.dbDir = db;
        this.stableLevel = stableLevel;
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
        List<Long> mergeParticipants = getFile2Merge(files);
        if( mergeParticipants.size() >= 5 ) {
            merge2StableFile( memTable2merge, files, fileBuffers, cache );
            return;
        }
        try
        {
            String targetFileName = Filename.unStableFileName( mergeParticipants.size() );
            builder.iterators().add( memTable2merge.iterator() );

            this.buildNewFile(targetFileName);


            this.closeUnused();
            this.evictUnused(cache);
            {
                this.fileMetaLock.writeLock().lock();
                // add new file meta info
                FileMetaData targetMetaData = new FileMetaData( mergeParticipants.size(), channel.size(), smallest, largest );
                files.put( targetMetaData.getNumber(), targetMetaData );
                //remove obsolete file meta info
                for( Long fileNumber : mergeParticipants ){
                    files.put( fileNumber, null );
                    fileBuffers.put( fileNumber, null );
                }
                this.fileMetaLock.writeLock().unlock();
            }
            this.stableLevel.dumpFileMeta2disc();
            this.unStableLevel.forceFileMetaToDisk();
            this.deleteUnused();
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
        List<Long> mergeParticipants = getFile2Merge(files);
        if( mergeParticipants.size() < 5 )
        {
            merge(memTable2merge, files, fileBuffers, cache);
            return;
        }
        try
        {
            String targetFileName = Filename.stableFileName(this.stableLevel.getNextFileNumber());
//            FileChannel targetChannel =
            TableBuilder builder = this.mergeInit(targetFileName);
            
            mergeIterators.add( memTable2merge.iterator() );

            SeekingIterator<Slice,Slice> lastFile = this.stableLevel.getlastFileIterator();
            if( null != lastFile )
                mergeIterators.add( this.stableLevel.getlastFileIterator() );

            MergingIterator buildIterator = getDataIterator(mergeParticipants, files, cache, fileBuffers);
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

            this.closeUnused();
            this.evictUnused(cache);
            {
                this.fileMetaLock.writeLock().lock();
                FileMetaData targetMetaData = new FileMetaData( this.stableLevel.getNextFileNumber(), targetChannel.size(), this.stableLevel.getlastBoundary(), largest );
                this.stableLevel.addFile( targetMetaData );
                for( Long fileNumber : mergeParticipants ) {
                    files.put( fileNumber, null );
                    fileBuffers.put( fileNumber, null );
                }
                this.fileMetaLock.writeLock().unlock();
            }
            this.stableLevel.dumpFileMeta2disc();
            this.unStableLevel.forceFileMetaToDisk();
            this.deleteUnused();
        }
        catch( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }

    public void setUnStableLevel(UnstableLevel unStableLevel) {
        this.unStableLevel = unStableLevel;
    }

    private List<Long> getFile2Merge(SortedMap<Long, FileMetaData> files) {
        List<Long> toMerge = new LinkedList<>();
        for( Long fileNo : new long[]{0,1,2,3,4} ) {
            FileMetaData metaData = files.get( fileNo );
            if( null == metaData ) break;
            else toMerge.add( fileNo );
        }
        return toMerge;
    }

    private class MergeTask{
        private final MemTable mem;
        private final SortedMap<Long, FileMetaData> files;
        private final SortedMap<Long, FileBuffer> fileBuffers;
        private final TableCache cache;

        private final List<SeekingIterator<Slice,Slice>> mergeIterators = new LinkedList<>();
        private final List<Closeable> channel2close = new LinkedList<>();
        private final List<File> files2delete = new LinkedList<>();
        private final List<Long> table2evict = new LinkedList<>();

        private MergeTask(MemTable memTable2merge,
                          SortedMap<Long,FileMetaData> files,
                          SortedMap<Long,FileBuffer> fileBuffers,
                          TableCache cache){
            this.mem = memTable2merge;
            this.files = files;
            this.fileBuffers = fileBuffers;
            this.cache = cache;
        }

        private void buildFrom(){}
        private TableBuilder mergeInit(String targetFileName) throws IOException
        {
            boolean success;

            File targetFile = new File( dbDir + "/" + targetFileName );
            if( targetFile.exists() ) {
                success = targetFile.delete();
                if (!success) {
                    throw new IOException("merge init error: fail to delete exist file");
                }
            }
            success = targetFile.createNewFile();
            if (success) {
                FileOutputStream targetStream = new FileOutputStream(targetFile);
                FileChannel targetChannel = targetStream.getChannel();
                this.channel2close.add( targetStream );
                this.channel2close.add( targetChannel );
                return new TableBuilder( new Options(), targetChannel, TableComparator.instance() );
            }else{
                throw new IOException("merge init error: fail to create file");
            }
        }

        private void mergeCleanUp(){
            table2evict.clear();
            files2delete.clear();
            channel2close.clear();
            mergeIterators.clear();
        }

        private void closeUnused() throws IOException {
            for( Closeable c : channel2close ) c.close();
        }

        private void evictUnused(TableCache cache) {
            for( Long num : table2evict ) cache.evict( num );
        }

        private void deleteUnused() throws IOException {
            for( File f : files2delete ) Files.delete( f.toPath() );
        }



        private MergingIterator getDataIterator(List<Long> mergeParticipants,
                                                Map<Long, FileMetaData> files, TableCache cache, Map<Long, FileBuffer> fileBuffers)
        {
            for( Long fileNumber : mergeParticipants )
            {
                FileMetaData metaData = files.get( fileNumber );
                File mergeFile = new File( dbDir + "/" + Filename.unStableFileName( metaData.getNumber() ) );

                table2evict.add( fileNumber );
                files2delete.add( mergeFile );

                FileBuffer filebuffer = fileBuffers.get( fileNumber );
                Table table = cache.newTable( fileNumber );
                SeekingIterator<Slice,Slice> mergeIterator;
                if( null != filebuffer )
                {
                    mergeIterator = new BufferFileAndTableIterator( filebuffer.iterator(),
                            table.iterator(), TableComparator.instance() );
                    channel2close.add( filebuffer );
                    files2delete.add( new File( dbDir + "/" + Filename.bufferFileName( fileNumber ) ) );
                }
                else
                {
                    mergeIterator = table.iterator();
                }
                channel2close.add( table );
                table2evict.add( fileNumber );
                mergeIterators.add( mergeIterator );
            }
            return new MergingIterator( mergeIterators, TableComparator.instance() );
        }

        private void buildNewFile(String targetFileName) {
            MergingIterator buildIterator = getDataIterator(mergeParticipants, files, cache, fileBuffers);

            int largest = -1, smallest = Integer.MAX_VALUE;
            int count = 0;
            TableBuilder builder = this.mergeInit(targetFileName);
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
        }

    }

}
