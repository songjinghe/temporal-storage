package org.act.temporalProperty.index.aggregation;

import com.google.common.collect.Lists;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.EntityTimeIntervalEntry;
import org.act.temporalProperty.index.IndexTableCache;
import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.index.SimplePoint2IntervalIterator;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.query.aggr.*;
import org.act.temporalProperty.query.aggr.IndexAggregationQuery.Duration;
import org.act.temporalProperty.query.aggr.IndexAggregationQuery.MinMax;
import org.act.temporalProperty.util.Slice;
import org.apache.commons.lang3.tuple.Pair;
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

    public long createDuration(PropertyMetaData pMeta, int start, int end, ValueGroupingMap valueGrouping, int every, int timeUnit) throws IOException {
        return durationIndex.create(pMeta, start, end, valueGrouping, every, timeUnit);
    }

    public long createMinMax(PropertyMetaData pMeta, int start, int end, int every, int timeUnit, IndexType type) throws IOException {
        return minMaxIndex.create(pMeta, start, end, every, timeUnit, type);
    }

    public AggregationIndexQueryResult query( long entityId, int proId, int start, int end, long indexId, MemTable cache ) throws IOException {
        AggregationIndexMeta meta = idMetaMap.get(indexId);
        if(meta==null || meta.getPropertyIdList().get(0)!=proId){
            throw new TPSRuntimeException("no index with id {}", indexId);
        } else {
            IndexType indexType = meta.getType();
            if (indexType == AGGR_DURATION) {
                return durationIndex.query(entityId, meta, start, end, cache);
            } else if (indexType == AGGR_MIN || indexType == AGGR_MAX || indexType == AGGR_MIN_MAX) {

                return minMaxIndex.query(entityId, meta, start, end, cache);
            }else{
                throw new TPSRuntimeException("index is not aggregation type!");
            }
        }
    }




    private List<Integer> pidList(PropertyMetaData meta){
        return Lists.newArrayList(meta.getPropertyId());
    }

    private Pair<Integer, Integer> overlappedGroups(AggregationIndexMeta meta, int start, int end){
        Entry<Integer, Integer> iStart = meta.getTimeGroupMap().ceilingEntry(start);
        Entry<Integer, Integer> iEnd = meta.getTimeGroupMap().floorEntry(end);
        if(iStart!=null && iEnd!=null && iStart.getKey()<iEnd.getKey()) {
            return Pair.of(iStart.getValue(), iEnd.getValue()-1);
        }else {
            return null;
        }
    }

    private List<Pair<Integer, Integer>> unCoveredTime(AggregationIndexMeta meta, int start, int end) {
        Integer iStart = meta.getTimeGroupMap().ceilingKey(start);
        Integer iEnd = meta.getTimeGroupMap().floorKey(end);
        List<Pair<Integer, Integer>> timeIntervalList = new ArrayList<>();
        if(iStart!=null && iEnd!=null && iStart<iEnd) {
            if(iStart>start) timeIntervalList.add(Pair.of(start, iStart-1));
            if(iEnd<end) timeIntervalList.add(Pair.of(iEnd, end));
        }else {
            timeIntervalList.add(Pair.of(start, end));
        }
        return timeIntervalList;
    }

    private class DurationIndexManager {

        public long create(PropertyMetaData pMeta, int start, int end, ValueGroupingMap valueGrouping, int every, int timeUnit) throws IOException {
            // 获得新索引文件的ID
            long indexId = nextId.getAndIncrement();
            // 索引文件
            File indexFile = new File(indexDir, Filename.aggrIndexFileName(indexId));
            // 获得构造索引文件需要的, 用于读取存储数据的iterator
            SearchableIterator raw = tpStore.buildIndexIterator(start, end, pidList(pMeta));
            // 将原始时间点Entry数据转换为时间区间Entry数据
            Iterator<EntityTimeIntervalEntry> interval = new SimplePoint2IntervalIterator(raw, end);
            // 计算索引的最小时间分块及时间块对应的ID
            TreeMap<Integer, Integer> timeGroup = AggregationIndexMeta.calcInterval(start, end, every, timeUnit);

            // 根据时间分块和value分区, 计算得出索引文件的Entry
            Iterator<AggregationIndexEntry> aggrEntries = new Interval2AggrEntryIterator(interval, valueGrouping.map(), timeGroup);
            // 将iterator的entry放入数组进行排序
            List<AggregationIndexEntry> data = Lists.newArrayList(aggrEntries);
            data.sort(Comparator.comparing(AggregationIndexEntry::getKey));
            // 排序后写入文件
            AggregationIndexFileWriter w = new AggregationIndexFileWriter(data, indexFile);
            long fileSize = w.write();
            //创建新的索引文件元信息
            AggregationIndexMeta meta = new AggregationIndexMeta(
                    indexId, AGGR_DURATION, pMeta.getPropertyId(), pMeta.getType(),
                    timeGroup.firstKey(), timeGroup.lastKey() - 1,
                    fileSize, every, timeUnit, valueGrouping.map());
            // 添加元信息到meta
            addMeta(meta);

            // 返回索引ID
            return indexId;
        }

        public AggregationIndexQueryResult query( long entityId, AggregationIndexMeta meta, int start, int end, MemTable cache ) throws IOException {
            TreeMap<Integer, Integer> result = new TreeMap<>();

            // 找出可以用索引加速的时间区间
            Pair<Integer, Integer> timeGroups = overlappedGroups(meta, start, end);
            if(timeGroups!=null){
                // 如果可以加速, 则使用索引得到这段时间的结果
                result = queryIndex(entityId, meta, timeGroups.getLeft(), timeGroups.getLeft());
            }
            // 找出不用索引加速的时间区间(列表)
            List<Pair<Integer, Integer>> unCoveredList = unCoveredTime(meta, start, end);
            if(!unCoveredList.isEmpty()) {
                // 根据索引信息(timeUnit, every, valueGroup)构建一个range查询
                DurationStatisticAggregationQuery packedQuery = packQuery(meta, end);
                int proId = meta.getPropertyIdList().get(0);
                for (Pair<Integer, Integer> time : unCoveredList) {
                    // 进行range查询并返回结果
                    TreeMap<Integer, Integer> rangeQueryResult = (TreeMap<Integer, Integer>) tpStore.getRangeValue(entityId, proId, start, end, packedQuery, cache);
                    // 合并结果
                    result = mergeAggrResult(result, rangeQueryResult);
                }
            }
            return new AggregationIndexQueryResult(result, timeGroups==null?0:(timeGroups.getRight()-timeGroups.getLeft()+1));
        }

        private TreeMap<Integer, Integer> queryIndex(long entityId, AggregationIndexMeta meta, int startTimeGroup, int endTimeGroup) throws IOException {
            TreeMap<Integer, Integer> result = new TreeMap<>();
            String filePath = new File(indexDir, Filename.aggrIndexFileName(meta.getId())).getAbsolutePath();
            SeekingIterator<Slice, Slice> iterator = cache.getTable(filePath).aggrIterator(filePath);

            iterator.seek(new AggregationIndexKey(entityId, startTimeGroup, 0).encode());
            while(iterator.hasNext()){
                Entry<Slice, Slice> entry = iterator.next();
                AggregationIndexKey key = new AggregationIndexKey(entry.getKey());
                if(key.getEntityId()==entityId && key.getTimeGroupId()>=startTimeGroup){
                    if(key.getTimeGroupId()<=endTimeGroup) {
                        result.putIfAbsent(key.getValueGroupId(), 0);
                        int duration = entry.getValue().getInt(0);
                        result.merge(key.getValueGroupId(), duration, (oldVal, newVal) -> oldVal + newVal);
                    }else{
                        return result;
                    }
                }
            }
            return result;
        }

        private DurationStatisticAggregationQuery packQuery(AggregationIndexMeta meta, int end) {
            TreeMap<Slice, Integer> vGroup = meta.getValGroupMap();

            return new DurationStatisticAggregationQuery<Integer>(end) {
                public Integer computeGroupId(TimeIntervalEntry entry) {
                    Entry<Slice, Integer> group = vGroup.floorEntry(entry.value());
                    return group==null ? -1 : group.getValue();
                }
                public Object onResult(Map result) {
                    return result;
                }
            };
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

    }



    private class MinMaxIndexManager {

        public long create(PropertyMetaData pMeta, int start, int end, int every, int timeUnit, IndexType type) throws IOException {
            // 获得新索引文件的ID
            long indexId = nextId.getAndIncrement();
            // 索引文件
            File indexFile = new File(indexDir, Filename.aggrIndexFileName(indexId));
            // 获得构造索引文件需要的, 用于读取存储数据的iterator
            SearchableIterator raw = tpStore.buildIndexIterator(start, end, pidList(pMeta));
            // 将原始时间点Entry数据转换为时间区间Entry数据
            Iterator<EntityTimeIntervalEntry> interval = new SimplePoint2IntervalIterator(raw, end);
            // 计算索引的最小时间分块及时间块对应的ID
            TreeMap<Integer, Integer> timeGroup = AggregationIndexMeta.calcInterval(start, end, every, timeUnit);

            // 根据时间分块和value分区, 计算得出索引文件的Entry(最大最小值)
            Iterator<Triple<Long,Integer,Slice>> minMax = new MinMaxAggrEntryIterator(interval, timeGroup);
            // 将iterator的entry放入数组进行排序
            List<Triple<Long,Integer,Slice>> data = Lists.newArrayList(minMax);
            data.sort(Triple::compareTo);
            // 排序后写入文件
            MinMaxAggrIndexWriter w = new MinMaxAggrIndexWriter(data, indexFile, ValueGroupingMap.getComparator(pMeta.getType()), type);
            long fileSize = w.write();
            // 添加元信息到meta
            addMeta(new AggregationIndexMeta(
                    indexId, type, pMeta.getPropertyId(), pMeta.getType(),
                    timeGroup.firstKey(), timeGroup.lastKey()-1, fileSize, every, timeUnit, new TreeMap<>()));

            // 返回索引ID
            return indexId;
        }


        public AggregationIndexQueryResult query( long entityId, AggregationIndexMeta meta, int start, int end, MemTable cache ) throws IOException {
            TreeMap<Integer, Slice> result = new TreeMap<>();
            Comparator<? super Slice> cp = ValueGroupingMap.getComparator(meta.getValueType());
            boolean shouldAddMin = (meta.getType()==AGGR_MIN || meta.getType()==AGGR_MIN_MAX);
            boolean shouldAddMax = (meta.getType()==AGGR_MAX || meta.getType()==AGGR_MIN_MAX);
            // 找出可以用索引加速的时间区间
            Pair<Integer, Integer> timeGroups = overlappedGroups(meta, start, end);
            if(timeGroups!=null){
                // 如果可以加速, 则使用索引得到这段时间的结果
                result = queryIndex(entityId, meta, cp, timeGroups.getLeft(), timeGroups.getLeft(), shouldAddMin, shouldAddMax);
            }
            // 找出不用索引加速的时间区间(列表)
            List<Pair<Integer, Integer>> unCoveredList = unCoveredTime(meta, start, end);
            if(!unCoveredList.isEmpty()) {
                // 根据索引信息(timeUnit, every, valueGroup)构建一个range查询
                AbstractTimeIntervalAggrQuery packedQuery = packQuery(meta, cp, end);
                int proId = meta.getPropertyIdList().get(0);
                for (Pair<Integer, Integer> time : unCoveredList) {
                    // 进行range查询并返回结果
                    TreeMap<Integer, Slice> rangeQueryResult = (TreeMap<Integer, Slice>) tpStore.getRangeValue(entityId, proId, start, end, packedQuery, cache);
                    // 合并结果
                    result = mergeAggrResult(result, rangeQueryResult, cp, shouldAddMin, shouldAddMax);
                }
            }
            return new AggregationIndexQueryResult(result, timeGroups==null?0:(timeGroups.getRight()-timeGroups.getLeft()+1), meta.getValueType());
        }

        private TreeMap<Integer, Slice> mergeAggrResult(TreeMap<Integer, Slice> map1, TreeMap<Integer, Slice> map2, Comparator<? super Slice> cp, boolean shouldAddMin, boolean shouldAddMax) {
            if(shouldAddMin){
                Slice min1 = map1.get(MinMax.MIN);
                Slice min2 = map2.get(MinMax.MIN);
                if(cp.compare(min2, min1)<0) map1.put(MinMax.MIN, min2);
            }
            if(shouldAddMax){
                Slice max1 = map1.get(MinMax.MAX);
                Slice max2 = map2.get(MinMax.MAX);
                if(cp.compare(max2, max1)>0) map1.put(MinMax.MAX, max2);
            }
            return map1;
        }

        private TreeMap<Integer, Slice> queryIndex(
                long entityId, AggregationIndexMeta meta, Comparator<? super Slice> cp, int startTimeGroup, int endTimeGroup, boolean shouldAddMin, boolean shouldAddMax) throws IOException {
            TreeMap<Integer, Slice> result = new TreeMap<>();
            String filePath = new File(indexDir, Filename.aggrIndexFileName(meta.getId())).getAbsolutePath();
            SeekingIterator<Slice, Slice> iterator = cache.getTable(filePath).aggrIterator(filePath);

            iterator.seek(new AggregationIndexKey(entityId, startTimeGroup, MinMax.MIN).encode());
            while(iterator.hasNext()){
                Entry<Slice, Slice> entry = iterator.next();
                AggregationIndexKey key = new AggregationIndexKey(entry.getKey());
                if(key.getEntityId()==entityId && key.getTimeGroupId()>=startTimeGroup){
                    if(key.getTimeGroupId()<=endTimeGroup) {
                        Slice val = entry.getValue();
                        if(shouldAddMin) result.merge(MinMax.MIN, val, (oldVal, newVal) -> (cp.compare(newVal, oldVal)<0) ? newVal : oldVal);
                        if(shouldAddMax) result.merge(MinMax.MAX, val, (oldVal, newVal) -> (cp.compare(newVal, oldVal)>0) ? newVal : oldVal);
                    }else{
                        return result;
                    }
                }
            }
            return result;
        }


        private AbstractTimeIntervalAggrQuery packQuery(AggregationIndexMeta meta, Comparator<? super Slice> cp, int end) {

            return new AbstractTimeIntervalAggrQuery<Integer, Slice>(end) {
                Slice min=null,max=null;
                @Override
                public Integer computeGroupId(TimeIntervalEntry entry) {
                    Slice val = entry.value();
                    if(min==null){
                        min = val;
                    }else if(cp.compare(val, min)<0){
                        min = val;
                        return null;
                    }//else do nothing.
                    if(max==null){
                        max = val;
                    }else if(cp.compare(val, max)>0){
                        max = val;
                        return null;
                    }//else do nothing.
                    return null;
                }

                @Override
                public Slice aggregate(Integer integer, Collection<TimeIntervalEntry> groupItems) {
                    return null;
                }

                public Object onResult(Map<Integer, Slice> result) {
                    result = new TreeMap<>();
                    result.put(MinMax.MAX, max);
                    result.put(MinMax.MIN, min);
                    return result;
                }
            };
        }
    }
}
