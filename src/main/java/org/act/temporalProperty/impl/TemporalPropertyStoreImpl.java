package org.act.temporalProperty.impl;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.helper.SameLevelMergeIterator;
import org.act.temporalProperty.helper.StoreInitial;
import org.act.temporalProperty.helper.EPEntryIterator;
import org.act.temporalProperty.helper.EPMergeIterator;
import org.act.temporalProperty.index.*;
import org.act.temporalProperty.index.value.IndexMetaData;
import org.act.temporalProperty.index.value.IndexQueryRegion;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.meta.SystemMeta;
import org.act.temporalProperty.meta.SystemMetaController;
import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;
import org.act.temporalProperty.query.aggr.IndexAggregationQuery;
import org.act.temporalProperty.query.range.InternalEntryRangeQueryCallBack;
import org.act.temporalProperty.table.TwoLevelMergeIterator;
import org.act.temporalProperty.table.MergeProcess;
import org.act.temporalProperty.table.TableBuilder;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.util.Slice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * TemporalPropertyStore的实现类
 *
 */
public class TemporalPropertyStoreImpl implements TemporalPropertyStore
{
    private SystemMeta meta;
    private MergeProcess mergeProcess;
    private File dbDir;
    private TableCache cache;
    private MemTable memTable;
    private MemTable stableMemTable; // a full memtable, which only used for query and (to be) merged, never write.
    private IndexStore index;

    private boolean forbiddenWrite=false;
    private FileReader lockFile; // keeps opened while system is running to prevent delete of the storage dir;

    /**
     * @param dbDir 存储动态属性数据的目录地址
     */
    public TemporalPropertyStoreImpl( File dbDir ) throws Throwable{
        this.dbDir = dbDir;
        this.init();
        this.cache = new TableCache( 25, TableComparator.instance(), false);
        this.meta.initStore(dbDir, cache);
        this.index = new IndexStore(new File(dbDir, "index"), this, meta.getIndexes(), meta.indexNextId());
        this.mergeProcess = new MergeProcess(dbDir.getAbsolutePath(), meta, index);
        this.mergeProcess.start();
    }

    /**
     * 系统启动时调用，主要作用是将上次系统关闭时写入磁盘的数据读入内存
     */
    private void init() throws Throwable
    {
        StoreInitial starter = new StoreInitial(dbDir);
        lockFile = starter.init();
        this.meta = starter.getMetaInfo();
        this.memTable = starter.getMemTable();
    }

    /**
     * 退出系统时调用，主要作用是将内存中的数据写入磁盘。
     */
    public void shutDown() throws IOException, InterruptedException {
        this.meta.lock.shutdown();
        this.mergeProcess.shutdown();
        this.cache.close();
        this.flushMemTable2Disk();
        this.flushMetaInfo2Disk();
        this.lockFile.close();
        Files.delete(new File(dbDir, Filename.lockFileName()).toPath());
    }


    @Override
    public Slice getPointValue(long entityId, int proId, int time ) {
        Slice idSlice = toSlice(proId, entityId);
        InternalKey searchKey = new InternalKey( idSlice, time );
        this.meta.lock.lockShared();
        try {
            Slice result = memTable.get(searchKey.encode());
            if (result != null && result.length() > 0) {
                return result;
            } else if (this.stableMemTable != null) {
                result = stableMemTable.get(searchKey.encode());
                if (result != null && result.length() > 0) {
                    return result;
                } else {
                    return meta.getStore(proId).getPointValue(idSlice, time);
                }
            } else {
                return meta.getStore(proId).getPointValue(idSlice, time);
            }
        }finally {
            this.meta.lock.unlockShared();
        }
    }

    @Override
    public Object getRangeValue( long id, int proId, int startTime, int endTime, InternalEntryRangeQueryCallBack callback ) {
        meta.lock.lockShared();
        try{
            Slice idSlice = toSlice(proId, id);

            SearchableIterator memIter = new EPEntryIterator(idSlice, new PackInternalKeyIterator(memTable.iterator()));
            if(this.stableMemTable!=null) memIter = new EPMergeIterator(idSlice, new PackInternalKeyIterator(stableMemTable.iterator()), memIter);
            SearchableIterator diskIter = meta.getStore(proId).getRangeValueIter(idSlice, startTime, endTime);
            SearchableIterator mergedIterator = new EPMergeIterator(idSlice, diskIter, memIter);

            InternalKey searchKey = new InternalKey( idSlice, startTime );
            mergedIterator.seek(searchKey);
            while(mergedIterator.hasNext()){
                InternalEntry entry = mergedIterator.next();
                InternalKey key = entry.getKey();
                if( key.getStartTime() <= endTime){
                    callback.onNewEntry(entry);
                }else break;
            }
            return callback.onReturn();
        }finally{
            meta.lock.unlockShared();
        }
    }

    @Override
    public boolean createProperty(int propertyId, ValueContentType type) {
        meta.lock.lockExclusive();
        try {
            SinglePropertyStore prop = meta.proStores().get(propertyId);
            if (prop == null) {
                try {
                    PropertyMetaData pMeta = new PropertyMetaData(propertyId, type);
                    meta.addStore(propertyId, new SinglePropertyStore(pMeta, dbDir, cache));
                    meta.addProperty(pMeta);
                    return true;
                } catch (Throwable ignore) {
                    return false;
                }
            } else {
                PropertyMetaData pMeta = meta.getProperties().get(propertyId);
                if (pMeta != null && pMeta.getType() == type) {
                    // already exist, maybe in recovery. so just delete all property then create again.
                    deleteProperty(propertyId);
                    createProperty(propertyId, type);
                    return true;
                } else {
                    throw new TPSRuntimeException("create temporal property failed, exist property with same id but diff type!");
                }
            }
        }finally {
            meta.lock.unlockExclusive();
        }
    }

    @Override
    public boolean setProperty( Slice key, byte[] value ){
        InternalKey internalKey = new InternalKey(key);
        if(!meta.getProperties().containsKey(internalKey.getPropertyId())){
            throw new TPSNHException("no such property id: "+internalKey.getPropertyId()+". should create first!");
        }

        meta.lock.lockExclusive();
        try{
            if(forbiddenWrite) meta.lock.waitSubmitMemTable();
            this.memTable.add(key, new Slice(value));
            if( this.memTable.approximateMemUsage() >= 4*1024*1024 ){
                forbiddenWrite = true;
                this.mergeProcess.add( this.memTable ); // may await at current line.
                this.stableMemTable = this.memTable;
                this.memTable = new MemTable( TableComparator.instance() );
                forbiddenWrite = false;
            }
            meta.lock.memTableSubmitted();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        } finally {
            meta.lock.unlockExclusive();
        }
        return true;
    }

    @Override
    public boolean deleteProperty(int propertyId) {
        meta.lock.lockExclusive();
        try{
            meta.getProperties().remove(propertyId);
            meta.getStore(propertyId).destroy();
            Set<IndexMetaData> indexSet = meta.getIndexes();
            for(IndexMetaData iMeta : indexSet){
                Set<Integer> pids = new HashSet<>(iMeta.getPropertyIdList());
                if(pids.contains(propertyId)) indexSet.remove(iMeta);
            }
            index.deleteIndex(propertyId);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            meta.lock.unlockExclusive();
        }
    }

    @Override
    public boolean deleteEntityProperty(Slice id) {
        //TODO deleteEntityProperty
        return false;
    }

    @Override
    public long createAggrDurationIndex(int propertyId, int start, int end, ValueGroupingMap valueGrouping, int every, int timeUnit){
        meta.lock.lockExclusive();
        try{
            PropertyMetaData pMeta = meta.getProperties().get(propertyId);
            return index.createAggrDurationIndex(pMeta, start, end, valueGrouping, every, timeUnit);
        } catch (IOException e) {
            e.printStackTrace();
            throw new TPSRuntimeException("error when create index.", e);
        } finally {
            meta.lock.unlockExclusive();
        }
    }

    @Override
    public long createAggrMinMaxIndex(int propertyId, int start, int end, int every, int timeUnit, IndexType type){
        meta.lock.lockExclusive();
        try{
            PropertyMetaData pMeta = meta.getProperties().get(propertyId);
            return index.createAggrMinMaxIndex(pMeta, start, end, every, timeUnit, type);
        } catch (IOException e) {
            e.printStackTrace();
            throw new TPSRuntimeException("error when create index.", e);
        } finally {
            meta.lock.unlockExclusive();
        }
    }

    @Override
    public Object aggregate(long entityId, int proId, int startTime, int endTime, InternalEntryRangeQueryCallBack callback) {
        return getRangeValue(entityId, proId, startTime, endTime, callback);
    }

    @Override
    public Object aggrWithIndex(long indexId, long entityId, int proId, int startTime, int endTime, IndexAggregationQuery query) {
        meta.lock.lockShared();
        try{
            PropertyMetaData pMeta = meta.getProperties().get(proId);
            return index.queryAggrIndex(entityId, pMeta, startTime, endTime, indexId, query);
        } catch (IOException e) {
            e.printStackTrace();
            throw new TPSRuntimeException("error when aggr with index.", e);
        } finally{
            meta.lock.unlockShared();
        }
    }

    @Override
    public long createValueIndex(int start, int end, List<Integer> proIds) {
        meta.lock.lockExclusive();
        try {
            List<IndexValueType> types = new ArrayList<>();
            for (Integer pid : proIds) {
                PropertyMetaData pMeta = meta.getProperties().get(pid);
                if (pMeta != null) {
                    types.add(IndexValueType.convertFrom(pMeta.getType()));
                } else {
                    throw new TPSRuntimeException("storage not contains property id " + pid);
                }
            }
            return createValueIndex(start, end, proIds, types);
        }finally {
            meta.lock.unlockExclusive();
        }
    }

    private long createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types){
        if(proIds.isEmpty()) throw new RuntimeException("should have at least one proId");
        meta.lock.lockExclusive();
        try {
            return index.createValueIndex(start, end, proIds, types);
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        } finally {
            meta.lock.unlockExclusive();
        }
    }

//    private TreeMap<Integer, Integer> mergeAggrQueryResult(Map<Integer, Integer> v1, Map<Integer, Integer> v2){
//        TreeMap<Integer, Integer> result = new TreeMap<>();
//        Set<Integer> keySet = v1.keySet();
//        keySet.addAll(v2.keySet());
//        for(Integer valGroupId : keySet){
//            int duration = 0;
//            Integer duration1 = v1.get(valGroupId);
//            if(duration1!=null) duration+=duration1;
//            Integer duration2 = v2.get(valGroupId);
//            if(duration2!=null) duration+=duration2;
//            result.put(valGroupId, duration);
//        }
//        return result;
//    }

//    private IndexEntryOperator extractOperator(IndexQueryRegion regions) {
//        List<IndexValueType> types = new ArrayList<>();
//        for(PropertyValueInterval p : regions.getPropertyValueIntervals()){
//            types.add(p.getType());
//        }
//        return new IndexEntryOperator(types, 4096);
//    }

    @Override
    public List<Long> getEntities(IndexQueryRegion condition) {
        List<IndexEntry> result = getEntries(condition);
        Set<Long> set = new HashSet<>();
        for(IndexEntry entry : result){
            set.add(entry.getEntityId());
        }
        return new ArrayList<>(set);
    }

    @Override
    public List<IndexEntry> getEntries(IndexQueryRegion condition) {
        try {
            return index.queryValueIndex(condition);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private SearchableIterator getMemTableIter(int start, int end){
        if(this.stableMemTable != null) {
            return TwoLevelMergeIterator.merge(memTable.iterator(), stableMemTable.iterator());
        }else{
            return new PackInternalKeyIterator(memTable.iterator());
        }
    }

    public SearchableIterator buildIndexIterator(int start, int end, List<Integer> proIds) {
        if(proIds.size()==1) {
            return new PropertyFilterIterator(proIds,
                    TwoLevelMergeIterator.toDisk(
                            getMemTableIter(start, end),
                            meta.getStore(proIds.get(0)).buildIndexIterator(start, end)));
        }else{
            SameLevelMergeIterator merged = new SameLevelMergeIterator();
            for(Integer pid : proIds){
                merged.add(meta.getStore(pid).buildIndexIterator(start, end));
            }
            return new PropertyFilterIterator(proIds, TwoLevelMergeIterator.toDisk( getMemTableIter(start, end), merged));
        }
    }


    private static Slice toSlice(int proId, long id) {
        Slice result = new Slice(12);
        result.setLong( 0, id );
        result.setInt( 8, proId );
        return result;
    }



    /**
     * 在系统关闭时，将MemTable中的数据写入磁盘
     * @Author Sjh
     * this is called when we need to manually start a merge process which force all data in memory to unstable file
     * on disk. we than create a new empty MemTable.
     * note that this method blocks current thread until all operation done.
     */
    @Override
    public void flushMemTable2Disk(){
        try{
            buffer2disk();
            File tempFile = new File( this.dbDir + "/" + Filename.tempFileName( 0 ));
            if( !tempFile.exists() )
                Files.createFile(tempFile.toPath());
            FileOutputStream outputStream = new FileOutputStream( tempFile );
            FileChannel channel = outputStream.getChannel();
            TableBuilder builer = new TableBuilder( new Options(), channel, TableComparator.instance() );
            SearchableIterator iterator = new PackInternalKeyIterator(this.memTable.iterator());
            while( iterator.hasNext() ){
                InternalEntry entry = iterator.next();
                builer.add( entry.getKey().encode(), entry.getValue() );
            }
            builer.finish();
            channel.close();
            outputStream.close();
        }catch( IOException e ){
            e.printStackTrace();
            throw new TPSRuntimeException("memTable flush failed", e);
        }
    }

    @Override
    public void flushMetaInfo2Disk(){
        try {
            SystemMetaController.forceToDisk(this.dbDir, this.meta);
        } catch (IOException e) {
            e.printStackTrace();
            throw new TPSRuntimeException("meta flush to disk failed", e);
        }
    }

    private void buffer2disk() throws IOException {
        for(PropertyMetaData p : this.meta.getProperties().values()){
            for(FileBuffer buffer : p.getUnstableBuffers().values()){
                buffer.close();
            }
            for(FileBuffer buffer : p.getStableBuffers().values()){
                buffer.close();
            }
        }
    }


}
