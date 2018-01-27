package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * Created by song on 2018-01-24.
 */
public class EPMergeIterator extends AbstractSeekingIterator<Slice, Slice> {
    private final Slice id;
    private final SeekingIterator<Slice, Slice> latest;
    private final SeekingIterator<Slice, Slice> old;

    public EPMergeIterator(Slice idSlice, SeekingIterator<Slice, Slice> old, SeekingIterator<Slice, Slice> latest) {
        this.id = idSlice;
        this.old = isEP(old)? old : new EPEntryIterator(idSlice, old);
        this.latest = isEP(latest) ? latest : new EPEntryIterator(idSlice, latest);
    }

    private boolean isEP(SeekingIterator<Slice, Slice> iterator) {
        return  (iterator instanceof EPEntryIterator) ||
                (iterator instanceof EPAppendIterator) ||
                (iterator instanceof EPMergeIterator);
    }

    @Override
    protected void seekToFirstInternal() {
        this.old.seekToFirst();
        this.latest.seekToFirst();
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        this.old.seek(targetKey);
        this.latest.seek(targetKey);
    }

    @Override
    protected Entry<Slice, Slice> getNextElement() {
        // remove head DELETION values in latest
        while (latest.hasNext()){
            InternalKey tmpKey = new InternalKey(latest.peek().getKey());
            if (tmpKey.getValueType()== ValueType.DELETION){
                latest.next(); // continue next loop;
            } else{
                break;
            }
        }

        if (latest.hasNext() && old.hasNext()){
            Entry<Slice,Slice> mem = latest.peek();
            Entry<Slice,Slice> disk = old.peek();

            if (time(mem) <= time(disk)){
                latest.next();
                if (latest.hasNext()){
                    pollUntil(old, time(latest.peek()));
                } else{
                    pollUntil(old, Integer.MAX_VALUE);
                }
                return mem;
            } else{
                old.next();
                return disk;
            }
        } else if (latest.hasNext()){ // diskIter run out
            return latest.next();
        } else if (old.hasNext()){ // memIter run out
            return old.next();
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
        while(iter.hasNext() && time(iter.peek()) < time){
            iter.next();
        }
    }
}
