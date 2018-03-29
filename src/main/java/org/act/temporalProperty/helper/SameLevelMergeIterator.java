package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;
import java.util.PriorityQueue;

/**
 * Used in merge process.
 * Note that although disk files' time ranges have no overlap, but their key (proId, entityId, time) range can overlap.
 * So we just use a PriorityQueue to manage this.
 *
 * Created by song on 2018-03-28.
 */
public class SameLevelMergeIterator extends AbstractIterator<InternalEntry> implements SearchableIterator {
    private PriorityQueue<SearchableIterator> heap = new PriorityQueue<>((o1, o2) -> {
        if(o1.hasNext() && o2.hasNext()){
            return o1.peek().getKey().compareTo(o2.peek().getKey());
        }else{
            throw new TPSNHException("iterators which ran out should not in heap!");
        }
    });

    public void add(SearchableIterator in){
        if(in.hasNext()) heap.add(in);
    }

    @Override
    protected InternalEntry computeNext() {
        SearchableIterator iter = heap.poll();
        if(iter!=null){
            InternalEntry entry = iter.next();
            if (iter.hasNext()) {
                heap.add(iter);
            }
            return entry;
        }else{
            return endOfData();
        }
    }

    @Override
    public void seekToFirst() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(InternalKey targetKey) {
        throw new UnsupportedOperationException();
    }
}
