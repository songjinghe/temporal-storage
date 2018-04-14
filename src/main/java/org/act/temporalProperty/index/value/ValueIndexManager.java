package org.act.temporalProperty.index.value;

import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.IndexTableCache;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.index.value.rtree.IndexEntryOperator;
import org.act.temporalProperty.util.TimeIntervalUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.Map.Entry;

import static org.act.temporalProperty.index.IndexType.*;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by song on 2018-04-07.
 */
public class ValueIndexManager {
    private final TemporalPropertyStoreImpl tpStore;
    private final IndexTableCache cache;
    private final File indexDir;
    private final AtomicLong nextId;

    private final Map<Integer, TreeMap<Integer, IndexMetaData>> singleVal = new HashMap<>(); // proId, time
    private final TreeMap<Integer, List<IndexMetaData>> multiVal = new TreeMap<>(); // time

    public ValueIndexManager(File indexDir, TemporalPropertyStoreImpl store, IndexTableCache cache, AtomicLong nextId) {
        this.indexDir = indexDir;
        this.tpStore = store;
        this.cache = cache;
        this.nextId = nextId;
    }

    public long createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types) throws IOException {
        long fileId = nextId.getAndIncrement();
        long fileSize = createValIndex(start, end, proIds, types, fileId);
        if (proIds.size() == 1) {
            addMeta(new IndexMetaData(fileId, SINGLE_VALUE, proIds, start, end, fileSize));
        } else {
            addMultiVal(new IndexMetaData(fileId, MULTI_VALUE, proIds, start, end, fileSize));
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

    public void addMeta(IndexMetaData indexMeta) {
        int pid = indexMeta.getPropertyIdList().get(0);
        singleVal.computeIfAbsent(pid, k -> new TreeMap<>());
        singleVal.get(pid).put(indexMeta.getTimeStart(), indexMeta);
    }

    public void addMultiVal(IndexMetaData meta) {
        multiVal.computeIfAbsent(meta.getTimeStart(), k -> new ArrayList<>());
        multiVal.get(meta.getTimeStart()).add(meta);
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
}
