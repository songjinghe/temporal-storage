package org.act.temporalProperty.index;

import com.google.common.base.Supplier;
import com.google.common.collect.*;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.index.IndexMetaData.SingleValIndexMeta;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.index.rtree.IndexEntryOperator;
import org.act.temporalProperty.table.BufferFileAndTableIterator;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.util.Slice;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by song on 2018-03-19.
 */
public class IndexStore {
    private IndexTableCache cache;
    private File indexDir;

    //meta
    private Map<Integer, TreeMap<Integer, IndexMetaData>> singleVal = new HashMap<>(); // proId, time
    private TreeMap<Integer, List<IndexMetaData>> multiVal = new TreeMap<>();
    private Map<Integer, TreeMap<Integer, IndexMetaData>> aggr = new HashMap<>(); // proId, time

    public IndexStore(File indexDir){
        this.indexDir = indexDir;
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
        for(List<IndexMetaData> lst : list) {
            for (IndexMetaData meta : lst) {

            }
        }
    }

    public void createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types) throws IOException {
        IndexMetaData indexMeta;
        if (proIds.size() == 1) {
            indexMeta = createSingleValIndex(start, end, proIds.get(0), types.get(0));
            addSingleValIndex(indexMeta);
        } else {

        }

    }



    public List<Long> getEntities(IndexQueryRegion condition){

    }

    public List<IndexEntry> getEntries(IndexQueryRegion condition){

    }

    private void addSingleValIndex(IndexMetaData indexMeta) {
        SingleValIndexMeta im = (SingleValIndexMeta) indexMeta;
        singleVal.computeIfAbsent(im.getProId(), k -> new TreeMap<>());
        singleVal.get(im.getProId()).put(indexMeta.getTimeStart(), indexMeta);
    }

    private IndexMetaData createSingleValIndex(int start, int end, int proId, IndexValueType types) throws IOException {
        IndexEntryOperator op = new IndexEntryOperator(Lists.newArrayList(types),4096);
        SeekingIterator<Slice,Slice> iterator = buildIndexIterator(start, end, Lists.newArrayList(proId));
        IndexBuilderCallback indexBuilderCallback = new IndexBuilderCallback(proId, op);
        while(iterator.hasNext()){
            Map.Entry<Slice, Slice> entry = iterator.next();
            InternalKey key = new InternalKey(entry.getKey());
            if(key.getValueType()== ValueType.INVALID) {
                indexBuilderCallback.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), null);
            }else{
                indexBuilderCallback.onCall(key.getPropertyId(), key.getEntityId(), key.getStartTime(), entry.getValue());
            }
        }
        PeekingIterator<IndexEntry> data = indexBuilderCallback.getIterator(end);

        FileChannel channel = new FileOutputStream(new File(this.indexDir, "index")).getChannel();
        IndexTableWriter writer = new IndexTableWriter(channel, op);
        while(data.hasNext()){
            writer.add(data.next());
        }
        writer.finish();
        channel.close();
        return new SingleValIndexMeta(proId, start, end);
    }


    private SeekingIterator<Slice, Slice> buildIndexIterator(int start, int end, List<Integer> proIds) {
        AppendIterator appendIterator = new AppendIterator();
        if( start < this.stLevel.getTimeBoundary() ) {
            int stEnd = Math.min((int) this.stLevel.getTimeBoundary(), end);
            appendIterator.append(this.stLevel.getRangeValueIter(start, stEnd));
        }
        if( end >= this.stLevel.getTimeBoundary() ) {
            int unStart = Math.max((int) this.stLevel.getTimeBoundary(), start);
            appendIterator.append(this.getRangeValueIter(unStart, end));
        }
        return new PropertyFilterIterator(
                proIds,
                new BufferFileAndTableIterator(
                        this.getMemTableIter(start, end),
                        appendIterator,
                        TableComparator.instance()));
    }


    public List<IndexEntry> valueIndexQuery(IndexQueryRegion condition) throws IOException {
        Iterator<IndexEntry> iter = this.index.iterator(condition);
        IndexEntryOperator op = extractOperator(condition);
        List<IndexEntry> result = new ArrayList<>();
        while(iter.hasNext()){
            result.add(iter.next());
        }
        return result;
    }

}
