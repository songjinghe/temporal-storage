package org.act.temporalProperty.index;

import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.index.rtree.IndexEntryOperator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.act.temporalProperty.index.IndexType.MULTI_VALUE;
import static org.act.temporalProperty.index.IndexType.SINGLE_VALUE;

/**
 * Created by song on 2018-03-19.
 */
public class IndexStore {
    private final TemporalPropertyStoreImpl tpStore;
    private IndexTableCache cache;
    private File indexDir;
    private AtomicInteger nextId = new AtomicInteger(0);
    //meta
    private Map<Integer, TreeMap<Integer, IndexMetaData>> singleVal = new HashMap<>(); // proId, time
    private TreeMap<Integer, List<IndexMetaData>> multiVal = new TreeMap<>(); // time
    private Map<Integer, TreeMap<Integer, IndexMetaData>> aggr = new HashMap<>(); // proId, time

    public IndexStore(File indexDir, TemporalPropertyStoreImpl store, Set<IndexMetaData> indexes) throws IOException {
        if(!indexDir.exists() && !indexDir.mkdir()) throw new IOException("unable to create index dir");
        this.indexDir = indexDir;
        this.tpStore = store;
        for(IndexMetaData meta : indexes){
            if(meta.getType()==SINGLE_VALUE){
                addSingleValIndex(meta);
            }else if(meta.getType()==MULTI_VALUE){
                addMultiValIndex(meta);
            }else{
                addAggrIndex(meta);
            }
        }
        this.cache = new IndexTableCache(indexDir, 4);
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

    public void createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types) throws IOException {
        IndexMetaData indexMeta;
        if (proIds.size() == 1) {
            indexMeta = createSingleValIndex(start, end, proIds.get(0), types.get(0));
            addSingleValIndex(indexMeta);
        } else {
            indexMeta = createMultiValIndex(start, end, proIds, types);
            addMultiValIndex(indexMeta);
        }
    }

    private IndexMetaData createMultiValIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types) throws IOException {
        IndexEntryOperator op = new IndexEntryOperator(Lists.newArrayList(types),4096);
        SearchableIterator iterator = tpStore.buildIndexIterator(start, end, proIds);
        IndexBuilderCallback indexBuilderCallback = new IndexBuilderCallback(proIds, op);
        while(iterator.hasNext()){
            InternalEntry entry = iterator.next();
            InternalKey key = entry.getKey();
            if(key.getValueType()== ValueType.INVALID) {
                indexBuilderCallback.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), null);
            }else{
                indexBuilderCallback.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), entry.getValue());
            }
        }
        PeekingIterator<IndexEntry> data = indexBuilderCallback.getIterator(start, end);

        try(FileChannel channel = new FileOutputStream(new File(this.indexDir, "index")).getChannel()) {
            IndexTableWriter writer = new IndexTableWriter(channel, op);
            while (data.hasNext()) {
                writer.add(data.next());
            }
            writer.finish();
            return new IndexMetaData(nextId.getAndIncrement(), MULTI_VALUE, proIds, start, end, channel.size());
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

    private void addAggrIndex(IndexMetaData indexMeta) {
        int pid = indexMeta.getPropertyIdList().get(0);
        aggr.computeIfAbsent(pid, k -> new TreeMap<>());
        aggr.get(pid).put(indexMeta.getTimeStart(), indexMeta);
    }


    private IndexMetaData createSingleValIndex(int start, int end, int proId, IndexValueType types) throws IOException {
        IndexEntryOperator op = new IndexEntryOperator(Lists.newArrayList(types),4096);
        SearchableIterator iterator = tpStore.buildIndexIterator(start, end, Lists.newArrayList(proId));
        IndexBuilderCallback indexBuilderCallback = new IndexBuilderCallback(Lists.newArrayList(proId), op);
        while(iterator.hasNext()){
            InternalEntry entry = iterator.next();
            InternalKey key = entry.getKey();
            if(key.getValueType()== ValueType.INVALID) {
                indexBuilderCallback.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), null);
            }else{
                indexBuilderCallback.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), entry.getValue());
            }
        }
        PeekingIterator<IndexEntry> data = indexBuilderCallback.getIterator(start, end);

        try(FileChannel channel = new FileOutputStream(new File(this.indexDir, "index")).getChannel()) {
            IndexTableWriter writer = new IndexTableWriter(channel, op);
            while (data.hasNext()) {
                writer.add(data.next());
            }
            writer.finish();
            return new IndexMetaData(nextId.getAndIncrement(), SINGLE_VALUE, Lists.newArrayList(proId), start, end, channel.size());
        }
    }

    public List<IndexEntry> valueIndexQuery(IndexQueryRegion condition) throws IOException {
        Iterator<IndexEntry> iter = this.getQueryIterator(condition);
        List<IndexEntry> result = new ArrayList<>();
        while(iter.hasNext()){
            result.add(iter.next());
        }
        return result;
    }

    //Fixme, naive version.
    private Iterator<IndexEntry> getQueryIterator(IndexQueryRegion condition) throws IOException {
        return cache.getTable(new File(this.indexDir, "index").getAbsolutePath()).iterator(condition);
    }

    public void deleteProperty(int propertyId) {
        //Fixme TODO
    }

    public void updateEntry(InternalEntry entry) {
        InternalKey key = entry.getKey();

    }
}
