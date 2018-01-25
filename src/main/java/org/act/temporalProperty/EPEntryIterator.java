package org.act.temporalProperty;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map;

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
        if(iter.hasNext()){
            if(validId(iter.peek())) {
                iter.next();
            }
        }
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        iter.seek(targetKey);
    }

    @Override
    protected Map.Entry<Slice, Slice> getNextElement() {
        if(iter.hasNext() && validId(iter.peek())){
            return iter.next();
        }else{
            return null;
        }
    }

    private boolean validId(Map.Entry<Slice, Slice> entry) {
        InternalKey tmpKey = new InternalKey(entry.getKey());
        return tmpKey.getId().equals(id);
    }
}
