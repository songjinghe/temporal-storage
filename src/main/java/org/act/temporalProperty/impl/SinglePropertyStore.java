package org.act.temporalProperty.impl;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.helper.EPAppendIterator;
import org.act.temporalProperty.helper.SameLevelMergeIterator;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.table.TwoLevelMergeIterator;
import org.act.temporalProperty.table.MergeProcess.MergeTask;
import org.act.temporalProperty.table.Table;
import org.act.temporalProperty.table.TableBuilder;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.util.FileUtils;
import org.act.temporalProperty.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Created by song on 2018-03-14.
 */
public class SinglePropertyStore
{
    private PropertyMetaData propertyMeta;
    private File proDir;
    private Logger log = LoggerFactory.getLogger( TemporalPropertyStoreImpl.class );
    private TableCache cache;

    /**
     * 实例化方法
     * @param dbDir 存储动态属性数据的目录地址
     */
    public SinglePropertyStore(PropertyMetaData propertyMeta, File dbDir, TableCache cache ) throws Throwable{
        this.propertyMeta = propertyMeta;
        this.proDir = new File(dbDir, propertyMeta.getPropertyId().toString());
        if(!proDir.exists() && !proDir.mkdir()) throw new IOException("create property dir failed: "+proDir.getAbsolutePath());
        this.cache = cache;
        this.loadBuffers();
    }

    private void loadBuffers() throws IOException {
        for(FileMetaData meta : propertyMeta.getUnStableFiles().values()){
            File bufferFile = new File(this.proDir, Filename.unbufferFileName(meta.getNumber()));
            if (bufferFile.exists()) {
                FileBuffer buffer = new FileBuffer(bufferFile);
                propertyMeta.addUnstableBuffer(meta.getNumber(), buffer);
            }
        }
        for(FileMetaData meta : propertyMeta.getStableFiles().values()){
            File bufferFile = new File(this.proDir, Filename.stbufferFileName(meta.getNumber()));
            if (bufferFile.exists()) {
                FileBuffer buffer = new FileBuffer(bufferFile);
                propertyMeta.addStableBuffer(meta.getNumber(), buffer);
            }
        }
    }

    /**
     * 进行时间点查询，参考{@link TemporalPropertyStore}中的说明
     */
    public Slice getPointValue(Slice idSlice, int time )
    {
        InternalKey searchKey = new InternalKey(idSlice, time);
        if( time > propertyMeta.stMaxTime() ){
            Slice result = this.unPointValue( searchKey );
            if( null == result || result.length() == 0 ) {
                return null;
            }else {
                return result;
            }
        }else {
            FileMetaData meta = propertyMeta.stHasTime(time);
            if(meta!=null) {
                return this.stPointValue(meta, searchKey);
            }else{
                return null;
            }
        }
    }

    EPAppendIterator getRangeValueIter(Slice idSlice, int startTime, int endTime)
    {
        List<FileMetaData> stList = propertyMeta.overlappedStable(startTime, endTime);
        List<FileMetaData> unList = propertyMeta.unFloorTime(endTime);
        stList.sort(Comparator.comparingInt(FileMetaData::getSmallest));
        unList.sort(Comparator.comparingInt(FileMetaData::getSmallest));

        EPAppendIterator iterator = new EPAppendIterator(idSlice);
        for(FileMetaData meta : stList){
            SearchableIterator fileIterator = this.cache.newIterator(Filename.stPath(proDir, meta.getNumber()));
            FileBuffer buffer = propertyMeta.getStableBuffers( meta.getNumber() );
            if( null != buffer ){
                iterator.append(TwoLevelMergeIterator.merge(buffer.iterator(), iterator));
            }else {
                iterator.append(fileIterator);
            }
        }
        for( FileMetaData meta : unList ){
            SearchableIterator fileIterator = this.cache.newIterator(Filename.unPath(proDir, meta.getNumber()));
            FileBuffer buffer = propertyMeta.getUnstableBuffers( meta.getNumber() );
            if( null != buffer ){
                iterator.append(TwoLevelMergeIterator.merge(buffer.iterator(), iterator));
            }else {
                iterator.append(fileIterator);
            }
        }
        return iterator;
    }

    private Slice unPointValue(InternalKey searchKey) {
        List<FileMetaData> checkList = propertyMeta.unFloorTime(searchKey.getStartTime());
        checkList.sort(Comparator.comparingInt(FileMetaData::getSmallest));
        for (FileMetaData meta : checkList) {
            SearchableIterator iterator = this.cache.newIterator(Filename.unPath(proDir, meta.getNumber()));
            FileBuffer buffer = propertyMeta.getUnstableBuffers(meta.getNumber());
            if (null != buffer) {
                iterator = TwoLevelMergeIterator.merge(buffer.iterator(), iterator);
            }
            iterator.seek(searchKey);
            Entry<InternalKey, Slice> entry;
            try {
                entry = iterator.next();
            } catch (NoSuchElementException e) {
                continue;
            }
            InternalKey resultKey = entry.getKey();
            if (resultKey.getId().equals(searchKey.getId()) && resultKey.getValueType() == ValueType.VALUE) {
                return entry.getValue();
            }
        }
        // search unstable complete but not found. now search latest stable file
        FileMetaData meta = propertyMeta.latestStableMeta();
        if(meta!=null) {
            return stPointValue(meta, searchKey);
        }else {
            return null;
        }
    }

    private Slice stPointValue(FileMetaData meta, InternalKey searchKey){
        SearchableIterator iterator = this.cache.newIterator(Filename.stPath(proDir, meta.getNumber()));
        FileBuffer buffer = propertyMeta.getStableBuffers(meta.getNumber());
        if (null != buffer) {
            iterator = TwoLevelMergeIterator.merge(buffer.iterator(), iterator);
        }
        iterator.seek(searchKey);
        Entry<InternalKey, Slice> entry;
        try {
            entry = iterator.next();
        } catch (NoSuchElementException e) {
            return null;
        }
        InternalKey resultKey = entry.getKey();
        if (resultKey.getId().equals(searchKey.getId()) && resultKey.getValueType() == ValueType.VALUE) {
            return entry.getValue();
        }else{
            return null;
        }
    }


    // this method runs in the background thread.
    // insert entry to file buffer, and pack remain entries to a MergeTask
    public MergeTask merge(MemTable memTable) throws IOException {
        SearchableIterator iterator = new PackInternalKeyIterator(memTable.iterator());
        MemTable stableMemTable = new MemTable(TableComparator.instance());
        boolean stExist = propertyMeta.hasStable();
        boolean unExist = propertyMeta.hasUnstable();
        while( iterator.hasNext() ){
            InternalEntry entry = iterator.next();
            InternalKey key = entry.getKey();
            int time = key.getStartTime();
            if( !unExist && !stExist ){
                stableMemTable.add(entry.getKey().encode(), entry.getValue());
            }else if( unExist && !stExist){
                if(time <= propertyMeta.unMaxTime()) {
                    insertUnstableBuffer(key, entry.getValue());
                }else{
                    stableMemTable.add(entry.getKey().encode(), entry.getValue());
                }
            }else if( unExist && stExist){
                if(time <= propertyMeta.stMaxTime()){
                    insertStableBuffer(key, entry.getValue());
                }else if(time <= propertyMeta.unMaxTime()){
                    insertUnstableBuffer(key, entry.getValue());
                }else{
                    stableMemTable.add(entry.getKey().encode(), entry.getValue());
                }
            }else{ // !unExist && stExist
                if(time <= propertyMeta.stMaxTime()){
                    insertStableBuffer(key, entry.getValue());
                }else{
                    stableMemTable.add(entry.getKey().encode(), entry.getValue());
                }
            }
        }
        if(!stableMemTable.isEmpty()){
            propertyMeta.updateMemTableMinTime( stableMemTable.getEndTime()+1 );
            return new MergeTask(proDir, stableMemTable, propertyMeta, this.cache);
        }else{
            return null;
        }
    }


    /**
     * 对某个已存在的UnStableFile的插入，插入到相应的Buffer中。如果不存在则新建一个Buffer
     * @param key
     * @param value
     */
    private void insertUnstableBuffer(InternalKey key, Slice value ) throws IOException
    {
        FileMetaData meta = propertyMeta.unFloorTimeOneMeta( key.getStartTime() );
        assert meta!=null : "SNH: meta should not null!";

        FileBuffer buffer = propertyMeta.getUnstableBuffers( meta.getNumber() );
        if( null == buffer ) {
            buffer = new FileBuffer(new File(this.proDir, Filename.unbufferFileName(meta.getNumber())));
            propertyMeta.addUnstableBuffer(meta.getNumber(), buffer);
        }
        buffer.add( key.encode(), value );
        if(buffer.size()>1024*1024*10) {
            unBufferToFile( meta, buffer );
        }
    }

    private void insertStableBuffer(InternalKey key, Slice value) throws IOException {
        FileMetaData meta = propertyMeta.stFloorTimeOneMeta( key.getStartTime() );
        assert meta!=null : "SNH: meta should not null!";

        FileBuffer buffer = propertyMeta.getStableBuffers( meta.getNumber() );
        if( null == buffer ) {
            buffer = new FileBuffer(new File(this.proDir, Filename.stbufferFileName(meta.getNumber())));
            propertyMeta.addStableBuffer(meta.getNumber(), buffer);
        }
        buffer.add( key.encode(), value );
        if(buffer.size()>1024*1024*10) {
            stBufferToFile( meta, buffer );
        }
    }


    private void unBufferToFile(FileMetaData meta, FileBuffer buffer) throws IOException {
        String filePath = Filename.unPath(proDir, meta.getNumber());
        String bufferPath = Filename.unbufferFileName(meta.getNumber());
        File tempFile = buffer2file(filePath, bufferPath, buffer);
        propertyMeta.delUnstableBuffer(meta.getNumber());
        if(!tempFile.renameTo(new File(filePath))) throw new IOException("rename failed!");
    }

    private void stBufferToFile(FileMetaData meta, FileBuffer buffer) throws IOException {
        String filePath = Filename.stPath(proDir, meta.getNumber());
        String bufferFileName = Filename.stbufferFileName(meta.getNumber());
        File tempFile = buffer2file(filePath, bufferFileName, buffer);
        propertyMeta.delStableBuffer(meta.getNumber());
        if(!tempFile.renameTo(new File(filePath))) throw new IOException("rename failed!");
    }

    private File buffer2file(String filePath, String bufferFileName, FileBuffer buffer) throws IOException {
        File tempFile = new File(this.proDir, Filename.tempFileName(6));
        if (tempFile.exists() && !tempFile.delete()) throw new IOException("can not delete tmp file!");
        if(!tempFile.createNewFile()) throw new IOException("can not create tmp file!");

        FileOutputStream stream = new FileOutputStream(tempFile);
        FileChannel channel = stream.getChannel();
        TableBuilder builder = new TableBuilder(new Options(), channel, TableComparator.instance());
        Table table = this.cache.newTable(filePath);
        SearchableIterator iterator = TwoLevelMergeIterator.merge(buffer.iterator(), table.iterator());
        while (iterator.hasNext()) {
            InternalEntry entry = iterator.next();
            builder.add(entry.getKey().encode(), entry.getValue());
        }
        builder.finish();
        channel.close();
        stream.close();
        table.close();
        this.cache.evict(filePath);
        File originFile = new File(filePath);
        Files.delete(originFile.toPath());
        buffer.close();
        Files.delete(new File(this.proDir, bufferFileName).toPath());
        return tempFile;
    }

    SameLevelMergeIterator buildIndexIterator(int startTime, int endTime) {
        List<FileMetaData> stList = propertyMeta.overlappedStable(startTime, endTime);
        List<FileMetaData> unList = propertyMeta.unFloorTime(endTime);
        stList.sort(Comparator.comparingInt(FileMetaData::getSmallest));
        unList.sort(Comparator.comparingInt(FileMetaData::getSmallest));

        SameLevelMergeIterator iterator = new SameLevelMergeIterator();
        for(FileMetaData meta : stList){
            SearchableIterator fileIterator = this.cache.newIterator(Filename.stPath(proDir, meta.getNumber()));
            FileBuffer buffer = propertyMeta.getStableBuffers( meta.getNumber() );
            if( null != buffer ){
                iterator.add(TwoLevelMergeIterator.merge(buffer.iterator(), fileIterator));
            }else {
                iterator.add(fileIterator);
            }
        }
        for( FileMetaData meta : unList ){
            SearchableIterator fileIterator = this.cache.newIterator(Filename.unPath(proDir, meta.getNumber()));
            FileBuffer buffer = propertyMeta.getUnstableBuffers( meta.getNumber() );
            if( null != buffer ){
                iterator.add(TwoLevelMergeIterator.merge(buffer.iterator(), fileIterator));
            }else {
                iterator.add(fileIterator);
            }
        }
        return iterator;
    }

    public void destroy() throws IOException {
        for(FileMetaData f : propertyMeta.getUnStableFiles().values()) {
            String path = Filename.unPath(proDir, f.getNumber());
            cache.evict(path);
        }
        for(FileMetaData f : propertyMeta.getStableFiles().values()) {
            String path = Filename.stPath(proDir, f.getNumber());
            cache.evict(path);
        }
        for(FileBuffer buffer : propertyMeta.getUnstableBuffers().values()){
            buffer.close();
        }
        for(FileBuffer buffer : propertyMeta.getStableBuffers().values()){
            buffer.close();
        }
        FileUtils.deleteRecursively(proDir);
    }


//    @Override
//    public boolean delete( Slice id )
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//




}