package org.act.temporalProperty.impl;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.helper.StoreInitial;
import org.act.temporalProperty.helper.EPEntryIterator;
import org.act.temporalProperty.helper.EPMergeIterator;
import org.act.temporalProperty.index.IndexQueryRegion;
import org.act.temporalProperty.index.IndexStore;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.PropertyValueInterval;
import org.act.temporalProperty.index.TimeRangeFilterIterator;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.index.rtree.IndexEntryOperator;
import org.act.temporalProperty.meta.SystemMeta;
import org.act.temporalProperty.meta.SystemMetaController;
import org.act.temporalProperty.table.BufferFileAndTableIterator;
import org.act.temporalProperty.table.MergeProcess;
import org.act.temporalProperty.table.TableBuilder;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.util.Slice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
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
    private IndexStore index;

    /**
     * @param dbDir 存储动态属性数据的目录地址
     */
    public TemporalPropertyStoreImpl( File dbDir ) throws Throwable{
        this.dbDir = dbDir;
        this.init();
        this.cache = new TableCache( 25, TableComparator.instance(), false);
        this.mergeProcess = new MergeProcess(dbDir.getAbsolutePath(), meta);
        this.mergeProcess.start();
        this.meta.initStore(dbDir, cache);
    }

    /**
     * 系统启动时调用，主要作用是将上次系统关闭时写入磁盘的数据读入内存
     */
    private void init() throws Throwable
    {
        StoreInitial starter = new StoreInitial(dbDir);
        this.meta = starter.getMetaInfo();
        this.memTable = starter.getMemTable();

    }

    /**
     * 退出系统时调用，主要作用是将内存中的数据写入磁盘。
     */
    public void shutDown()
    {
        this.flushMemTable2Disk();
        this.flushMetaInfo2Disk();
    }


    @Override
    public Slice getPointValue( long id, int proId, int time ) {
        Slice idSlice = toSlice(proId, id);
        InternalKey searchKey = new InternalKey( idSlice, time );
        this.meta.lockShared();
        try {
            Slice result = memTable.get(searchKey.encode());
            if (result != null && result.length() > 0) {
                return result;
            } else {
                return meta.getStore(proId).getPointValue(idSlice, time);
            }
        }finally {
            this.meta.unLockShared();
        }
    }

    @Override
    public Object getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback ) {
        meta.lockShared();
        try{
            Slice idSlice = toSlice(proId, id);

            SeekingIterator<Slice,Slice> memIter = new EPEntryIterator(idSlice, memTable.iterator());
            SeekingIterator<Slice,Slice> diskIter = meta.getStore(proId).getRangeValueIter(idSlice, startTime, endTime);
            SeekingIterator<Slice,Slice> mergedIterator = new EPMergeIterator(idSlice, diskIter, memIter);

            InternalKey searchKey = new InternalKey( idSlice, startTime );
            mergedIterator.seek(searchKey.encode());
            while(mergedIterator.hasNext()){
                Entry<Slice,Slice> entry = mergedIterator.next();
                InternalKey key = new InternalKey( entry.getKey() );
                if( key.getStartTime() <= endTime && key.getValueType() == ValueType.VALUE){
                    callback.onCall(key.getStartTime(), entry.getValue());
                }else break;
            }
            return callback.onReturn();
        }finally{
            meta.unLockShared();
        }
    }

    private SeekingIterator<Slice,Slice> getMemTableIter(int start, int end){
        if(this.stableMemTable != null) {
            return new TimeRangeFilterIterator(
                    new BufferFileAndTableIterator(memTable.iterator(), stableMemTable.iterator(), TableComparator.instance()),
                    start, end );
        }else{
            return new TimeRangeFilterIterator(memTable.iterator(), start, end);
        }
    }

    public SeekingIterator<Slice,Slice> getMemTableIter(Slice idSlice){
        if(this.stableMemTable != null) {
            return new EPMergeIterator(idSlice, stableMemTable.iterator(), memTable.iterator());
        }else{
            return new EPEntryIterator(idSlice, memTable.iterator());
        }
    }

    @Override
    public boolean setProperty( Slice key, byte[] value ){
        if( this.memTable.approximateMemoryUsage() >= 4*1024*1024 ){
            while( this.mergeProcess.isMerging()){
                try{
                    Thread.sleep(200);
                }catch ( InterruptedException e ){
                    //FIXME
                    e.printStackTrace();
                }
            }
        }
        this.memTable.add(key, new Slice(value));
        if( !this.mergeProcess.isMerging() && this.memTable.approximateMemoryUsage() >= 4*1024*1024 ){
            MemTable temp = this.memTable;
            this.mergeProcess.offer( temp );
            this.memTable = new MemTable( TableComparator.instance() );
        }
        return true;
    }

    @Override
    public boolean delete( Slice id )
    {
        // TODO Auto-generated method stub
        return false;
    }


    public void createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types){
        if(proIds.isEmpty()) throw new RuntimeException("should have at least one proId");
        meta.lockShared();
        try {
            index.createValueIndex(start, end, proIds, types);
        }finally {
            meta.unLockShared();
        }
    }



    private IndexEntryOperator extractOperator(IndexQueryRegion regions) {
        List<IndexValueType> types = new ArrayList<>();
        for(PropertyValueInterval p : regions.getPropertyValueIntervals()){
            types.add(p.getType());
        }
        return new IndexEntryOperator(types, 4096);
    }

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
            return unLevel.valueIndexQuery(condition);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static Slice toSlice(int proId, long id) {
        Slice result = new Slice(12);
        result.setInt( 0, proId );
        result.setLong( 4, id );
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
            File tempFile = new File( this.dbDir + "/" + Filename.tempFileName( 0 ));
            if( !tempFile.exists() )
                tempFile.createNewFile();
            FileOutputStream outputStream = new FileOutputStream( tempFile );
            FileChannel channel = outputStream.getChannel();
            TableBuilder builer = new TableBuilder( new Options(), channel, TableComparator.instance() );
            SeekingIterator<Slice,Slice> iterator = this.memTable.iterator();
            while( iterator.hasNext() ){
                Entry<Slice,Slice> entry = iterator.next();
                builer.add( entry.getKey(), entry.getValue() );
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


}
