package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.aggregation.*;
import org.act.temporalProperty.index.value.*;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.query.aggr.AggregationIndexQueryResult;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;
import org.act.temporalProperty.util.SliceOutput;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by song on 2018-03-19.
 */
public class IndexStore {
    private final TemporalPropertyStoreImpl tpStore;
    private IndexTableCache cache;

    private AggregationIndexOperator aggr;
    private ValueIndexOperator value;
    private IndexMetaManager meta;

    public IndexStore(File indexDir, TemporalPropertyStoreImpl store, Set<IndexMetaData> indexes, long nextId, long nextFileId) throws IOException {
        if(!indexDir.exists() && !indexDir.mkdir()) throw new IOException("unable to create index dir");
        this.tpStore = store;
        this.cache = new IndexTableCache(indexDir, 4);
        this.meta = new IndexMetaManager( indexes, nextId, nextFileId );
        this.aggr = new AggregationIndexOperator( indexDir, store, cache, meta );
        this.value = new ValueIndexOperator( indexDir, store, cache, meta );
    }

    public void close(){
        this.cache.close();
    }

    public void encode(SliceOutput out){
        //TODO fixme
    }

//    public List<IndexMetaData> availableSingleValIndexes(int proId, int start, int end){
//        TreeMap<Integer, IndexMetaData> map = singleVal.get(proId);
//        if(map==null){
//            return Collections.emptyList();
//        }else{
//            return new ArrayList<>(map.subMap(start, true, end, true).values());
//        }
//    }
//
//    public List<IndexMetaData> availableMultiValIndexes(List<Integer> proIds, int start, int end){
//        proIds.sort(Integer::compareTo);
//        Collection<List<IndexMetaData>> list = multiVal.subMap(start, true, end, true).values();
//        List<IndexMetaData> result = new ArrayList<>();
//        for(List<IndexMetaData> lst : list) {
//            result.addAll(lst);
//        }
//        return result;
//    }

    public long createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types) throws IOException {
        return value.create(start, end, proIds, types);
    }

    public long createAggrDurationIndex(PropertyMetaData pMeta, int start, int end, ValueGroupingMap valueGrouping, int every, int timeUnit) throws IOException {
        return aggr.createDuration(pMeta, start, end, valueGrouping, every, timeUnit);
    }

    public long createAggrMinMaxIndex(PropertyMetaData pMeta, int start, int end, int every, int timeUnit, IndexType type) throws IOException {
        return aggr.createMinMax(pMeta, start, end, every, timeUnit, type);
    }

    public List<IndexEntry> queryValueIndex(IndexQueryRegion condition) throws IOException {
        return value.query(condition);
    }

    public AggregationIndexQueryResult queryAggrIndex( long entityId, PropertyMetaData meta, int start, int end, long indexId, MemTable cache ) throws IOException {
        return aggr.query(entityId, meta.getPropertyId(), start, end, indexId, cache);
    }

    public void deleteIndex(int propertyId) {
        //Fixme TODO
    }

    /**
     * update index if needed.
     */
    public List<BackgroundTask> createNewIndexTasks()
    {
        List<BackgroundTask> result = new ArrayList<>();
        result.addAll( this.value.createIndexTasks() );
        result.addAll( this.aggr.createIndexTasks() );
        return result;
    }

    public boolean isOnline( long indexId )
    {
        return false;
    }

    public IndexUpdater onBufferDelUpdate( int propertyId, boolean isStable, FileMetaData fMeta, MemTable mem )
    {
        List<IndexFileMeta> fileToUpdate = new ArrayList<>();
        List<IndexMetaData> indexes = meta.getByProId( propertyId );
        for ( IndexMetaData i : indexes )
        {
            if ( i.getType() == IndexType.MULTI_VALUE )
            {
                for(IndexFileMeta fileMeta : i.allFiles())
                {
                    if(mem.overlap( fileMeta.getStartTime(), fileMeta.getEndTime() ))
                    {
                        fileToUpdate.add( fileMeta );
                    }
                }
            }
            else
            {
                IndexFileMeta fileMeta = i.getByFileId( fMeta.getNumber() );
                if ( fileMeta != null )
                {
                    if(mem.overlap( fileMeta.getStartTime(), fileMeta.getEndTime() ))
                    {
                        fileToUpdate.add( fileMeta );
                    }
                }
            }
        }
        if ( fileToUpdate.isEmpty() )
        {
            return emptyUpdate();
        }
        return new IndexUpdater( meta, propertyId, isStable, fMeta.getNumber(), fileToUpdate );
    }

    public IndexUpdater onMergeUpdate( int propertyId, boolean isStable, long newStorefileId, MemTable mem, List<Long> mergeParticipants )
    {
        List<IndexFileMeta> fileToDelete = new ArrayList<>();
        List<IndexMetaData> indexes = meta.getByProId( propertyId );
        for ( IndexMetaData i : indexes )
        {
            if ( i.getType() == IndexType.MULTI_VALUE )
            {
                for(IndexFileMeta fileMeta : i.allFiles())
                {
                    if(mem.overlap( fileMeta.getStartTime(), fileMeta.getEndTime() ))
                    {
                        fileToDelete.add( fileMeta );
                    }
                }
            }
            else
            {
                for(Long obsoleteFileId : mergeParticipants)
                {
                    IndexFileMeta fileMeta = i.getByFileId( obsoleteFileId );
                    if ( fileMeta != null )
                    {
                        fileToDelete.add( fileMeta );
                    }
                }
            }
        }
        if ( fileToDelete.isEmpty() )
        {
            return emptyUpdate();
        }
        return new IndexUpdater( meta, propertyId, isStable, newStorefileId, fileToDelete );
    }

    public IndexUpdater emptyUpdate()
    {
        return new IndexUpdater()
        {
            public void update( InternalEntry entry )
            {
            }

            public void updateMeta()
            {
            }

            public void cleanUp()
            {
            }
        };
    }
}
