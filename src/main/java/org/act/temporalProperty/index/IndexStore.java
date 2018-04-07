package org.act.temporalProperty.index;

import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.aggregation.*;
import org.act.temporalProperty.index.value.*;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.index.value.rtree.IndexEntryOperator;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.query.aggr.AggregationIndexKey;
import org.act.temporalProperty.query.aggr.DurationStatisticAggregationQuery;
import org.act.temporalProperty.query.aggr.IndexAggregationQuery;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.TimeIntervalUtil;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import static org.act.temporalProperty.index.IndexType.*;

/**
 * Created by song on 2018-03-19.
 */
public class IndexStore {
    private final TemporalPropertyStoreImpl tpStore;
    private IndexTableCache cache;
    private File indexDir;
    private AtomicLong nextId;
    //meta
    private Map<Integer, TreeMap<Integer, IndexMetaData>> singleVal = new HashMap<>(); // proId, time
    private TreeMap<Integer, List<IndexMetaData>> multiVal = new TreeMap<>(); // time
//    private Map<Integer, TreeMap<Integer, AggregationIndexMeta>> aggr = new HashMap<>(); // proId, time
    private Map<Long, AggregationIndexMeta> aggr = new HashMap<>(); // indexId,

    public IndexStore(File indexDir, TemporalPropertyStoreImpl store, Set<IndexMetaData> indexes, long nextId) throws IOException {
        if(!indexDir.exists() && !indexDir.mkdir()) throw new IOException("unable to create index dir");
        this.indexDir = indexDir;
        this.tpStore = store;
        for(IndexMetaData meta : indexes){
            if(meta.getType()==SINGLE_VALUE){
                addSingleValIndex(meta);
            }else if(meta.getType()==MULTI_VALUE){
                addMultiValIndex(meta);
            }else{
                addAggrIndex((AggregationIndexMeta) meta);
            }
        }
        this.cache = new IndexTableCache(indexDir, 4);
        this.nextId = new AtomicLong(nextId);
    }


    public List<IndexMetaData> availableSingleValIndexes(int proId, int start, int end){
        TreeMap<Integer, IndexMetaData> map = singleVal.get(proId);
        if(map==null){
            return Collections.emptyList();
        }else{
            return new ArrayList<>(map.subMap(start, true, end, true).values());
        }
    }

    public List<IndexMetaData> availableMultiValIndexes(List<Integer> proIds, int start, int end){
        proIds.sort(Integer::compareTo);
        Collection<List<IndexMetaData>> list = multiVal.subMap(start, true, end, true).values();
        List<IndexMetaData> result = new ArrayList<>();
        for(List<IndexMetaData> lst : list) {
            result.addAll(lst);
        }
        return result;
    }

    public long createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types) throws IOException {
        long fileId = nextId.getAndIncrement();
        long fileSize = createValIndex(start, end, proIds, types, fileId);
        if (proIds.size() == 1) {
            addSingleValIndex(new IndexMetaData(fileId, SINGLE_VALUE, proIds, start, end, fileSize));
        } else {
            addMultiValIndex(new IndexMetaData(fileId, MULTI_VALUE, proIds, start, end, fileSize));
        }
        return fileId;
    }

    private long createValIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types, long fileId) throws IOException {
        IndexEntryOperator op = new IndexEntryOperator(Lists.newArrayList(types),4096);
        SearchableIterator iterator = tpStore.buildIndexIterator(start, end, proIds);
        IndexBuilderCallback dataCollector = new IndexBuilderCallback(proIds, op);
        while(iterator.hasNext()){
            InternalEntry entry = iterator.next();
            InternalKey key = entry.getKey();
            if(key.getValueType()== ValueType.INVALID) {
                dataCollector.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), null);
            }else{
                dataCollector.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), entry.getValue());
            }
        }
        PeekingIterator<IndexEntry> data = dataCollector.getIterator(start, end);

        String indexFilePath = Filename.valIndexFileName(fileId);
        try(FileChannel channel = new FileOutputStream(new File(this.indexDir, indexFilePath)).getChannel()) {
            IndexTableWriter writer = new IndexTableWriter(channel, op);
            while (data.hasNext()) {
                writer.add(data.next());
            }
            writer.finish();
            return channel.size();
        }
    }

    private void addSingleValIndex(IndexMetaData indexMeta) {
        int pid = indexMeta.getPropertyIdList().get(0);
        singleVal.computeIfAbsent(pid, k -> new TreeMap<>());
        singleVal.get(pid).put(indexMeta.getTimeStart(), indexMeta);
    }

    private void addMultiValIndex(IndexMetaData meta) {
        multiVal.computeIfAbsent(meta.getTimeStart(), k -> new ArrayList<>());
        multiVal.get(meta.getTimeStart()).add(meta);
    }

    public long createAggrIndex(PropertyMetaData pMeta, int start, int end, ValueGroupingMap valueGrouping, int every, int timeUnit) throws IOException {
        long indexId = nextId.getAndIncrement();
        List<Integer> pidList = Lists.newArrayList(pMeta.getPropertyId());
        SearchableIterator iterator = tpStore.buildIndexIterator(start, end, pidList);
        File indexFile = new File(indexDir, Filename.aggrIndexFileName(indexId));
        Iterator<EntityTimeIntervalEntry> point2interval = new SimplePoint2IntervalIterator(iterator, end);
        TreeMap<Integer, Integer> intervalStarts = AggregationIndexMeta.calcInterval(start, end, every, timeUnit);
        long fileSize = createAggrDurationIndex(point2interval, indexFile, pMeta.getType(), valueGrouping, intervalStarts);
        AggregationIndexMeta meta = new AggregationIndexMeta(indexId, AGGR_DURATION, pidList.get(0), intervalStarts.firstKey(), intervalStarts.lastKey()-1, fileSize, every, timeUnit, valueGrouping.map());
        addAggrIndex(meta);
        return indexId;
    }

    private long createAggrDurationIndex(
            Iterator<EntityTimeIntervalEntry> timeIntervalIter,
            File indexFile,
            ValueContentType type,
            ValueGroupingMap valueGrouping,
            TreeMap<Integer, Integer> intervalStarts) throws IOException {
        TreeMap<Slice, Integer> vMap = valueGrouping.map();
        Iterator<AggregationIndexEntry> aggrIter = new Interval2AggrEntryIterator(timeIntervalIter, vMap, intervalStarts);
        List<AggregationIndexEntry> data = new ArrayList<>();
        while(aggrIter.hasNext()){
            data.add(aggrIter.next());
        }
        data.sort(Comparator.comparing(AggregationIndexEntry::getKey));
        AggregationIndexFileWriter w = new AggregationIndexFileWriter(data, indexFile);
        return w.write();
    }

    private void addAggrIndex(AggregationIndexMeta indexMeta) {
        aggr.putIfAbsent(indexMeta.getId(), indexMeta);
    }

    public List<IndexEntry> valueIndexQuery(IndexQueryRegion condition) throws IOException {
        IndexMetaData meta = valIndexCoverRegion(condition);
        Iterator<IndexEntry> iter = this.getQueryIterator(condition, meta);
        List<IndexEntry> result = new ArrayList<>();
        while(iter.hasNext()){
            result.add(iter.next());
        }
        return result;
    }

    private IndexMetaData valIndexCoverRegion(IndexQueryRegion condition) {
        List<PropertyValueInterval> proIntervals = condition.getPropertyValueIntervals();
        if(proIntervals.size()==1){
            int pid = proIntervals.get(0).getProId();
            TreeMap<Integer, IndexMetaData> tmap = singleVal.get(pid);
            if(tmap!=null && !tmap.isEmpty()) {
                Entry<Integer, IndexMetaData> meta = tmap.floorEntry(condition.getTimeMin());
                if(meta != null){
                    int indexStart = meta.getValue().getTimeStart();
                    int indexEnd = meta.getValue().getTimeEnd();
                    if(TimeIntervalUtil.contains( indexStart, indexEnd, condition.getTimeMin(), condition.getTimeMax())){
                        return meta.getValue();
                    }else{
                        throw new TPSRuntimeException(
                                "property {} index range {}-{} not fully cover query region {}-{}!",
                                pid, indexStart, indexEnd, condition.getTimeMin(), condition.getTimeMax());
                    }
                }else{
                    throw new TPSRuntimeException(
                            "property {} has no index fully cover query region {}-{}!",
                            pid, condition.getTimeMin(), condition.getTimeMax());
                }
            }else{
                throw new TPSRuntimeException("property {} has no index!", pid);
            }
        }else if(!proIntervals.isEmpty()){
            Integer t = multiVal.floorKey(condition.getTimeMin());
            if(t!=null) {
                Set<Integer> conditionPidSet = new HashSet<>();
                for(PropertyValueInterval p : proIntervals){
                    conditionPidSet.add(p.getProId());
                }
                List<IndexMetaData> metaList = multiVal.get(t);
                for(IndexMetaData meta : metaList){
                    int s = meta.getTimeStart();
                    int e = meta.getTimeEnd();
                    if(TimeIntervalUtil.contains( s, e, condition.getTimeMin(), condition.getTimeMax())){
                        Set<Integer> pSetMeta = new HashSet<>(meta.getPropertyIdList());
                        if(pSetMeta.size()==conditionPidSet.size() && !pSetMeta.retainAll(conditionPidSet)) {
                            return meta;
                        }
                    }
                }
                throw new TPSRuntimeException("no valid index for query!");
            }else{
                throw new TPSRuntimeException("no valid index for query!");
            }
        }else {
            throw new TPSRuntimeException("no valid index for query!");
        }
    }

    private Iterator<IndexEntry> getQueryIterator(IndexQueryRegion condition, IndexMetaData meta) throws IOException {
        String fileName = Filename.valIndexFileName(meta.getId());
        return cache.getTable(new File(this.indexDir, fileName).getAbsolutePath()).iterator(condition);
    }

    public void deleteProperty(int propertyId) {
        //Fixme TODO
    }

    public void updateEntry(InternalEntry entry) {
        InternalKey key = entry.getKey();

    }

    public Object aggrIndexQuery(long entityId, int proId, int start, int end, long indexId, IndexAggregationQuery query) throws IOException {
        AggregationIndexMeta meta = aggr.get(indexId);
        if(meta!=null){
            Entry<Integer, Integer> iStart = meta.getTimeGroupMap().ceilingEntry(start);
            Entry<Integer, Integer> iEnd = meta.getTimeGroupMap().floorEntry(end);
            if(iStart!=null && iEnd!=null && iStart.getKey()!=iEnd.getKey()) {

                if(meta.getType()==AGGR_DURATION && query instanceof IndexAggregationQuery.Duration) {
                    return aggrIndexDurationQuery();
                }else{

                }


            }else{
                return tpStore.aggregate(entityId, proId, start, end, query);
            }
        }else{
            throw new TPSRuntimeException("no index for property {}", proId);
        }
    }

    private TreeMap<Integer, Integer> mergeAggrResult(TreeMap<Integer, Integer> a, TreeMap<Integer, Integer> b) {
        TreeMap<Integer, Integer> smaller = (a.size()<b.size()? a : b);
        TreeMap<Integer, Integer> larger = (smaller==a ? b : a);
        for(Entry<Integer, Integer> entry : smaller.entrySet()){
            int valGroupId = entry.getKey();
            int sumOfDuration = entry.getValue();
            larger.merge(valGroupId, sumOfDuration, (old, cur) -> old+cur);
        }
        return larger;
    }

    private Object aggrIndexDurationQuery(AggregationIndexMeta meta, int proId, long entityId, int iStart, int iEnd, int start, int end) {
        TreeMap<Integer, Integer> result1=null, result2=null,
                indexQueryResult = aggrIndexQuery(entityId, meta, iStart, iEnd);
        if(iStart>start || iEnd<end) {
            DurationStatisticAggregationQuery packedQuery = packQuery(meta);
            if(iStart>start) result1 = (TreeMap<Integer, Integer>) tpStore.aggregate(entityId, proId, start, iStart-1, packedQuery);
            if(iEnd<end) result2 = (TreeMap<Integer, Integer>) tpStore.aggregate(entityId, proId, iEnd, end, packedQuery);
        }
        if(result1!=null) indexQueryResult = mergeAggrResult(result1, indexQueryResult);
        if(result2!=null) indexQueryResult = mergeAggrResult(result2, indexQueryResult);
        return query.onResult(indexQueryResult);
        return null;
    }

    private DurationStatisticAggregationQuery packQuery(AggregationIndexMeta meta) {
        return new DurationStatisticAggregationQuery<>() {
            public Object computeGroupId(TimeIntervalEntry entry) {
                //return null;
            }
            public Object onResult(Map result) {
                //return null;
            }
        };
    }

    private TreeMap<Integer, Integer> aggrIndexQuery(long entityId, AggregationIndexMeta meta, int startTime, int endTime) throws IOException {
        TreeMap<Integer, Integer> result = new TreeMap<>();
        String filePath = new File(indexDir, Filename.aggrIndexFileName(meta.getId())).getAbsolutePath();
        SeekingIterator<Slice, Slice> iterator = cache.getTable(filePath).aggrIterator(filePath);

        iterator.seek(new AggregationIndexKey(entityId, startTimeGroup, -1).encode());
        while(iterator.hasNext()){
            Entry<Slice, Slice> entry = iterator.next();
            AggregationIndexKey key = new AggregationIndexKey(entry.getKey());
            if(key.getEntityId()==entityId && key.getTimeGroupId()>=startTimeGroup && key.getTimeGroupId()<=endTimeGroup){
                result.putIfAbsent(key.getValueGroupId(), 0);
                int duration = entry.getValue().getInt(0);
                result.merge(key.getValueGroupId(), duration, (oldVal, newVal) -> oldVal+newVal);
            }
        }
        return result;
    }

    public long createAggrMinMaxIndex(PropertyMetaData pMeta, int start, int end, int every, int timeUnit, IndexType type) throws IOException {
        long indexId = nextId.getAndIncrement();
        List<Integer> pidList = Lists.newArrayList(pMeta.getPropertyId());
        SearchableIterator iterator = tpStore.buildIndexIterator(start, end, pidList);
        File indexFile = new File(indexDir, Filename.aggrIndexFileName(indexId));
        Iterator<EntityTimeIntervalEntry> point2interval = new SimplePoint2IntervalIterator(iterator, end);
        TreeMap<Integer, Integer> intervalStarts = AggregationIndexMeta.calcInterval(start, end, every, timeUnit);
        long fileSize = createAggrMinMaxIndex(point2interval, indexFile, intervalStarts, pMeta.getType(), type);
        AggregationIndexMeta meta = new AggregationIndexMeta(indexId, type, pidList.get(0), intervalStarts.firstKey(), intervalStarts.lastKey()-1, fileSize, every, timeUnit, new TreeMap<>());
        addAggrIndex(meta);
        return indexId;
    }


    private long createAggrMinMaxIndex(
            Iterator<EntityTimeIntervalEntry> iterator, File indexFile, TreeMap<Integer, Integer> intervalStarts, ValueContentType propertyType, IndexType type) throws IOException {
        List<Triple<Long,Integer,Slice>> data = new ArrayList<>();
        Iterator<Triple<Long,Integer,Slice>> aggrIter = new MinMaxAggrEntryIterator(iterator, intervalStarts);
        while(aggrIter.hasNext()){
            data.add(aggrIter.next());
        }
        data.sort(Triple::compareTo);
        MinMaxAggrIndexWriter w = new MinMaxAggrIndexWriter(data, indexFile, ValueGroupingMap.getComparator(propertyType), type);
        return w.write();
    }


}
