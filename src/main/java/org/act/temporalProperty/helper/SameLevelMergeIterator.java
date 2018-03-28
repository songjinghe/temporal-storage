package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.exception.TPSNHException;
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
public class SameLevelMergeIterator extends AbstractIterator<Entry<Slice,Slice>> implements SeekingIterator<Slice,Slice> {
    private PriorityQueue<SeekingIterator<Slice,Slice>> heap = new PriorityQueue<>((o1, o2) -> {
        if(o1.hasNext() && o2.hasNext()){
            return TableComparator.instance().compare(o1.peek().getKey(), o2.peek().getKey());
        }else{
            throw new TPSNHException("iterators which ran out should not in heap!");
        }
    });

    public void add(SeekingIterator<Slice, Slice> in){
        if(in.hasNext()) heap.add(in);
    }

    @Override
    protected Entry<Slice, Slice> computeNext() {
        SeekingIterator<Slice, Slice> iter = heap.poll();
        if(iter!=null){
            Entry<Slice, Slice> entry = iter.next();
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
    public void seek(Slice targetKey) {
        throw new UnsupportedOperationException();
    }
}
