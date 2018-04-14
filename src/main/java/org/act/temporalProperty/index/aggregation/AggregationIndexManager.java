package org.act.temporalProperty.index.aggregation;

import com.google.common.collect.Lists;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.EntityTimeIntervalEntry;
import org.act.temporalProperty.index.IndexTableCache;
import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.index.SimplePoint2IntervalIterator;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.query.aggr.AggregationIndexKey;
import org.act.temporalProperty.query.aggr.DurationStatisticAggregationQuery;
import org.act.temporalProperty.query.aggr.IndexAggregationQuery;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;
import org.act.temporalProperty.util.Slice;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import static org.act.temporalProperty.index.IndexType.*;
/**
 * Created by song on 2018-04-07.
 */
public class AggregationIndexManager {
    private final TemporalPropertyStoreImpl tpStore;
    private final IndexTableCache cache;
    private final File indexDir;
    private final AtomicLong nextId;

    private final Map<Long, AggregationIndexMeta> idMetaMap = new HashMap<>(); // indexId,
    private final DurationIndexManager durationIndex = new DurationIndexManager();
    private final MinMaxIndexManager minMaxIndex = new MinMaxIndexManager();

    public AggregationIndexManager(File indexDir, TemporalPropertyStoreImpl store, IndexTableCache cache, AtomicLong nextId) {
        this.indexDir = indexDir;
        this.tpStore = store;
        this.cache = cache;
        this.nextId = nextId;
    }

    public void addMeta(AggregationIndexMeta indexMeta) {
        idMetaMap.putIfAbsent(indexMeta.getId(), indexMeta);
    }

    public long createAggrIndex(PropertyMetaData pMeta, int start, int end, ValueGroupingMap valueGrouping, int every, int timeUnit) throws IOException {
        long indexId = nextId.getAndIncrement();
        List<Integer> pidList = Lists.newArrayList(pMeta.getPropertyId());
        SearchableIterator iterator = tpStore.buildIndexIterator(start, end, pidList);
        File indexFile = new File(indexDir, Filename.aggrIndexFileName(indexId));
        Iterator<EntityTimeIntervalEntry> point2interval = new SimplePoint2IntervalIterator(iterator, end);
        TreeMap<Integer, Integer> intervalStarts = AggregationIndexMeta.calcInterval(start, end, every, timeUnit);
        long fileSize = durationIndex.create(point2interval, indexFile, pMeta.getType(), valueGrouping, intervalStarts);
        addMeta(new AggregationIndexMeta(indexId, AGGR_DURATION, pidList.get(0), intervalStarts.firstKey(), intervalStarts.lastKey()-1, fileSize, every, timeUnit, valueGrouping.map()));
        return indexId;
    }


    public Object aggrIndexQuery(long entityId, int proId, int start, int end, long indexId, IndexAggregationQuery query) throws IOException {
        AggregationIndexMeta meta = idMetaMap.get(indexId);
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
        long fileSize = minMaxIndex.create(point2interval, indexFile, intervalStarts, pMeta.getType(), type);
        addMeta(new AggregationIndexMeta(indexId, type, pidList.get(0), intervalStarts.firstKey(), intervalStarts.lastKey()-1, fileSize, every, timeUnit, new TreeMap<>()));
        return indexId;
    }




    private class DurationIndexManager {
        private long create(
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
    }

    private class MinMaxIndexManager {
        private long create(
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
    }
}
