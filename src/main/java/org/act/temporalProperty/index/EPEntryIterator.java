package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by song on 2018-01-24.
 */
public class EPEntryIterator extends AbstractSeekingIterator<Slice, Slice> {

    private final SeekingIterator<Slice, Slice> iter;
    private final Slice id;

    public EPEntryIterator(Slice entityPropertyId, SeekingIterator<Slice, Slice> iterator){
        this.id = entityPropertyId;
        this.iter = iterator;
        this.seekToFirstInternal();
    }

    @Override
    protected void seekToFirstInternal() {
        InternalKey earliestKey = new InternalKey(id, 0, 0, ValueType.VALUE);
        iter.seek(earliestKey.encode());
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        iter.seek(targetKey);
    }

    @Override
    protected Entry<Slice, Slice> getNextElement() {
        while(iter.hasNext()){
            Entry<Slice, Slice> entry = iter.next();
            if(validId(entry)){
                return entry;
            }
        }
        return null;
    }

    private boolean validId(Entry<Slice, Slice> entry) {
        InternalKey tmpKey = new InternalKey(entry.getKey());
        return tmpKey.getId().equals(id);
    }
}
