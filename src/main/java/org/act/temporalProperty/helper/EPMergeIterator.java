package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * Created by song on 2018-01-24.
 */
public class EPMergeIterator extends AbstractIterator<InternalEntry> implements SearchableIterator {
    private final Slice id;
    private final SearchableIterator latest;
    private final SearchableIterator old;

    public EPMergeIterator(Slice idSlice, SearchableIterator old, SearchableIterator latest) {
        this.id = idSlice;
        this.old = isEP(old)? old : new EPEntryIterator(idSlice, old);
        this.latest = isEP(latest) ? latest : new EPEntryIterator(idSlice, latest);
    }

    private boolean isEP(SearchableIterator iterator) {
        return  (iterator instanceof EPEntryIterator) ||
                (iterator instanceof EPAppendIterator) ||
                (iterator instanceof EPMergeIterator);
    }

    @Override
    public void seekToFirst() {
        this.old.seekToFirst();
        this.latest.seekToFirst();
    }

    @Override
    public void seek(InternalKey targetKey) {
        this.old.seek(targetKey);
        this.latest.seek(targetKey);
    }

    private int time(InternalEntry entry){
        return entry.getKey().getStartTime();
    }

    private void pollUntil(SearchableIterator iter, int time){
        InternalKey tmp = new InternalKey(id, time, 0, ValueType.VALUE);
        iter.seek(tmp);
        while(iter.hasNext() && time(iter.peek()) < time){
            iter.next();
        }
    }

    @Override
    protected InternalEntry computeNext() {
        if (latest.hasNext() && old.hasNext()){
            InternalEntry mem = latest.peek();
            InternalEntry disk = old.peek();

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
}
