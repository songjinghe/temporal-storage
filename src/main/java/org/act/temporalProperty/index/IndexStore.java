package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.aggregation.*;
import org.act.temporalProperty.index.value.*;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.query.aggr.IndexAggregationQuery;
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
    private File indexDir;
    private AtomicLong nextId;

    private AggregationIndexManager aggr;
    private ValueIndexManager value;

    public IndexStore(File indexDir, TemporalPropertyStoreImpl store, Set<IndexMetaData> indexes, long nextId) throws IOException {
        if(!indexDir.exists() && !indexDir.mkdir()) throw new IOException("unable to create index dir");
        this.indexDir = indexDir;
        this.tpStore = store;
        this.cache = new IndexTableCache(indexDir, 4);
        this.nextId = new AtomicLong(nextId);
        this.aggr = new AggregationIndexManager(indexDir, store, cache, this.nextId);
        this.value = new ValueIndexManager(indexDir, store, cache, this.nextId);
        for(IndexMetaData meta : indexes){
            if(meta.getType().isValueIndex()){
                value.addMeta(meta);
            }else{
                aggr.addMeta((AggregationIndexMeta) meta);
            }
        }
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

    public Object queryAggrIndex(long entityId, PropertyMetaData meta, int start, int end, long indexId, IndexAggregationQuery query) throws IOException {
        return aggr.query(entityId, meta, start, end, indexId, query);
    }

    public void deleteIndex(int propertyId) {
        //Fixme TODO
    }

    public void updateEntry(InternalEntry entry) {
        InternalKey key = entry.getKey();
    }

}
