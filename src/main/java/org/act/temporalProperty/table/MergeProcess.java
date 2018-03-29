package org.act.temporalProperty.table;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.Map.Entry;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.meta.SystemMeta;
import org.act.temporalProperty.util.MergingIterator;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.TableLatestValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件合并过程
 *
 */
public class MergeProcess extends Thread
{
    private final SystemMeta systemMeta;
    private final String storeDir;
    private volatile MemTable memTable = null;
    private static Logger log = LoggerFactory.getLogger( MergeProcess.class );

    public MergeProcess(String storePath, SystemMeta systemMeta ) {
        this.storeDir = storePath;
        this.systemMeta = systemMeta;
    }

    // this is called from a writer thread.
    // the caller should get write lock first.
    public void add(MemTable memTable) throws InterruptedException{
        while(this.memTable!=null){
            systemMeta.writeDiskComplete.await();
        }
        this.memTable = memTable;
    }

    @Override
    public void run(){
        Thread.currentThread().setName("TemporalPropStore-"+(storeDir.endsWith("temporal.node.properties")?"Node":"Rel"));
        try{
            while(!Thread.interrupted()) {
                if (memTable!=null && !memTable.isEmpty()) {
                    startMergeProcess(memTable);
                }else{
                    Thread.sleep(100);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        catch (Throwable e ){
//            e.printStackTrace();
//            log.error( "error happens when dump memtable to disc", e );
//        }
    }

    /**
     * 触发数据写入磁盘，如果需要还需要对文件进行合并
     * @param temp 需要写入磁盘的MemTable
     * @throws IOException
     */
    private void startMergeProcess( MemTable temp ) throws IOException
    {
        SeekingIterator<Slice,Slice> iterator = temp.iterator();
        Map<Integer, MemTable> tables = new HashMap<>();
        while( iterator.hasNext() ){
            Entry<Slice,Slice> entry = iterator.next();
            InternalKey key = new InternalKey( entry.getKey() );
            if(!tables.containsKey(key.getPropertyId())){
                tables.put(key.getPropertyId(), new MemTable( TableComparator.instance() ));
            }
            tables.get(key.getPropertyId()).add(entry.getKey(), entry.getValue());
        }

        List<MergeTask> taskList = new LinkedList<>();
        for(Entry<Integer, MemTable> propEntry : tables.entrySet()){
            MergeTask task = systemMeta.getStore(propEntry.getKey()).merge(propEntry.getValue());
            if(task!=null){
                task.buildNewFile();
                taskList.add(task);
            }
        }

        systemMeta.lockExclusive();
        try {
            for (MergeTask task : taskList) task.updateMetaInfo();
            systemMeta.force(new File(storeDir));
            memTable = null;
            systemMeta.writeDiskComplete.signalAll();
        }finally {
            systemMeta.unLockExclusive();
        }

        for(MergeTask task : taskList){
            task.deleteObsoleteFiles();
        }
    }

    // 将MemTable写入磁盘并与UnStableFile进行合并
    public static class MergeTask{
        private final File propStoreDir;
        private final MemTable mem;
        private final TableCache cache;
        private final List<Long> mergeParticipants;
        private final PropertyMetaData pMeta;

        private final List<SeekingIterator<Slice,Slice>> mergeIterators = new LinkedList<>();
        private final List<Closeable> channel2close = new LinkedList<>();
        private final List<File> files2delete = new LinkedList<>();
        private final List<String> table2evict = new LinkedList<>();

        private int entryCount;
        private int minTime;
        private int maxTime;
        private FileChannel targetChannel;

        /**
         * @param memTable2merge 写入磁盘的MemTable
         * @param proMeta 属性元信息
         * @param cache 用来读取UnStableFile的缓存结构
         */
        public MergeTask(File propStoreDir, MemTable memTable2merge, PropertyMetaData proMeta, TableCache cache){
            this.propStoreDir = propStoreDir;
            this.mem = memTable2merge;
            this.pMeta = proMeta;
            this.cache = cache;
            this.mergeParticipants = getFile2Merge(proMeta.getUnStableFiles());
        }

        private TableBuilder mergeInit(String targetFileName) throws IOException
        {
            boolean success;

            File targetFile = new File( propStoreDir, targetFileName );
            if( targetFile.exists() ) {
                success = targetFile.delete();
                if (!success) {
                    throw new IOException("merge init error: fail to delete exist file");
                }
            }
            success = targetFile.createNewFile();
            if (success) {
                FileOutputStream targetStream = new FileOutputStream(targetFile);
                targetChannel = targetStream.getChannel();
                this.channel2close.add( targetStream );
                this.channel2close.add( targetChannel );
                return new TableBuilder( new Options(), targetChannel, TableComparator.instance() );
            }else{
                throw new IOException("merge init error: fail to create file");
            }
        }

        private void closeUnused() throws IOException {
            for( Closeable c : channel2close ) c.close();
        }

        private void evictUnused(TableCache cache) {
            for( String filePath : table2evict ) cache.evict( filePath );
        }

        public void deleteObsoleteFiles() throws IOException {
            for( File f : files2delete ) Files.delete( f.toPath() );
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

        private SeekingIterator<Slice,Slice> getDataIterator(){
            if(onlyDumpMemTable()) {
                return this.mem.iterator();
            }else{

                List<SeekingIterator<Slice,Slice>> list = new ArrayList<>();

                for (Long fileNumber : mergeParticipants) {
//                    log.debug("merge {}", fileNumber);
                    File mergeSource = new File(propStoreDir, Filename.unStableFileName(fileNumber));
                    Table table = cache.newTable(mergeSource.getAbsolutePath());
                    SeekingIterator<Slice, Slice> mergeIterator;
                    FileBuffer filebuffer = pMeta.getUnstableBuffers(fileNumber);
                    if (null != filebuffer) {
                        mergeIterator = TwoLevelMergeIterator.merge(filebuffer.iterator(), table.iterator());
                        channel2close.add(filebuffer);
                        files2delete.add(new File(propStoreDir, Filename.unbufferFileName(fileNumber)));
                    } else {
                        mergeIterator = table.iterator();
                    }
                    list.add(mergeIterator);

                    table2evict.add(mergeSource.getAbsolutePath());
                    files2delete.add(mergeSource);
                    channel2close.add(table);
                }
                MergingIterator unstableIter = new MergingIterator(list, TableComparator.instance());
                SeekingIterator<Slice, Slice> diskDataIter;
                if (createStableFile() && pMeta.hasStable()) {
                    int mergeResultStartTime = pMeta.getUnStableFiles().get(Collections.max(mergeParticipants)).getSmallest();
                    diskDataIter = TwoLevelMergeIterator.merge(unstableIter, stableLatestValIter(mergeResultStartTime));
                } else {
                    diskDataIter = unstableIter;
                }
                return TwoLevelMergeIterator.toDisk(this.mem.iterator(), diskDataIter);
            }
        }

        public boolean createStableFile(){
            return mergeParticipants.size()>=5;
        }

        public boolean onlyDumpMemTable(){
            return mergeParticipants.isEmpty();
        }

        public void buildNewFile() throws IOException {
            maxTime = -1;
            minTime = Integer.MAX_VALUE;
            entryCount = 0;

            String targetFileName;
            if(createStableFile()) {
                targetFileName = Filename.stableFileName(pMeta.nextStableId());
            }else{
                targetFileName = Filename.unStableFileName( mergeParticipants.size() );
            }

            TableBuilder builder = this.mergeInit(targetFileName);
            SeekingIterator<Slice,Slice> buildIterator = getDataIterator();
            while( buildIterator.hasNext() ){
                Entry<Slice,Slice> entry = buildIterator.next();
                InternalKey key = new InternalKey( entry.getKey() );
                if( key.getStartTime() < minTime ) minTime = key.getStartTime();
                if( key.getStartTime() > maxTime ) maxTime = key.getStartTime();
                builder.add( entry.getKey(), entry.getValue() );
                entryCount++;
            }
            builder.finish();
        }

        public void updateMetaInfo() throws IOException {
            // remove old meta
            for( Long fileNumber : mergeParticipants ){
                pMeta.delUnstable( fileNumber );
                pMeta.delUnstableBuffer( fileNumber );
            }

            // add new meta to level
            long fileNumber; int startTime;
            if(createStableFile()){
                fileNumber = pMeta.nextStableId();
                if(pMeta.hasStable()) {
                    startTime=pMeta.stMaxTime()+1;
                }else{
                    startTime=0;
                }
                FileMetaData targetMeta = new FileMetaData( fileNumber, targetChannel.size(), startTime, maxTime );
                pMeta.addStable(targetMeta);
            }else{
                fileNumber = mergeParticipants.size();
                FileMetaData targetMeta = new FileMetaData( fileNumber, targetChannel.size(), minTime, maxTime );
                pMeta.addUnstable(targetMeta);
            }

            closeUnused();
            evictUnused(cache);
        }

        // this should only be called when pMeta.hasStable() is true.
        private SeekingIterator<Slice, Slice> stableLatestValIter(int mergeResultStartTime) {
            FileMetaData meta = pMeta.latestStableMeta();
            String filePath = Filename.stPath(propStoreDir, meta.getNumber());
            SeekingIterator<Slice, Slice> fileIterator = cache.newTable(filePath).iterator();
            FileBuffer buffer = pMeta.getStableBuffers( meta.getNumber() );
            if( null != buffer ){
                fileIterator = TwoLevelMergeIterator.merge(buffer.iterator(), fileIterator);
            }
            return TableLatestValueIterator.setNewStart(fileIterator, mergeResultStartTime);
        }


    }

}
