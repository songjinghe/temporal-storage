package org.act.temporalProperty.index;

import org.act.temporalProperty.EPEntryIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by song on 2018-01-24.
 */
public class EPMergeIterator extends AbstractSeekingIterator<Slice, Slice> {
    private final Slice id;
    private final SeekingIterator<Slice, Slice> high;
    private final SeekingIterator<Slice, Slice> low;

    public EPMergeIterator(Slice idSlice, SeekingIterator<Slice, Slice> low, SeekingIterator<Slice, Slice> high) {
        this.id = idSlice;
        this.low = isEP(low)? low : new EPEntryIterator(idSlice, low);
        this.high = isEP(high) ? high : new EPEntryIterator(idSlice, high);
    }

    private boolean isEP(SeekingIterator<Slice, Slice> iterator) {
        return iterator instanceof EPEntryIterator || iterator instanceof EPAppendIterator;
    }

    @Override
    protected void seekToFirstInternal() {
        this.low.seekToFirst();
        this.high.seekToFirst();
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        this.low.seek(targetKey);
        this.high.seek(targetKey);
    }

    @Override
    protected Entry<Slice, Slice> getNextElement() {
        // remove head DELETION values in high
        while (high.hasNext()){
            InternalKey tmpKey = new InternalKey(high.peek().getKey());
            if (tmpKey.getValueType()== ValueType.DELETION){
                high.next(); // continue next loop;
            } else{
                break;
            }
        }

        if (high.hasNext() && low.hasNext()){
            Entry<Slice,Slice> mem = high.peek();
            Entry<Slice,Slice> disk = low.peek();

            if (time(mem) <= time(disk)){
                high.next();
                if (high.hasNext()){
                    pollUntil(low, time(high.peek()));
                } else{
                    pollUntil(low, Integer.MAX_VALUE);
                }
                return mem;
            } else{
                low.next();
                return disk;
            }
        } else if (high.hasNext()){ // diskIter run out
            return high.next();
        } else if (low.hasNext()){ // memIter run out
            return low.next();
        } else{ // both ran out
            return null;
        }
    }

    private int time(Entry<Slice,Slice> entry){
        InternalKey key = new InternalKey(entry.getKey());
        return key.getStartTime();
    }

    private void pollUntil(SeekingIterator<Slice,Slice> iter, int time){
        InternalKey tmp = new InternalKey(id, time, 0, ValueType.VALUE);
        iter.seek(tmp.encode());
        if(iter.hasNext() && time(iter.peek()) < time){
            iter.next();
        }
    }
}
