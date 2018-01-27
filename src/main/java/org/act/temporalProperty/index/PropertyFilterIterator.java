package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Created by song on 2018-01-25.
 */
public class PropertyFilterIterator extends AbstractSeekingIterator<Slice,Slice> {

    private final Set<Integer> idSet;
    private final SeekingIterator<Slice, Slice> iterator;

    public PropertyFilterIterator(List<Integer> proIds, SeekingIterator<Slice,Slice> iterator){
        this.idSet = new HashSet<>(proIds);
        this.iterator = iterator;
    }

    @Override
    protected void seekToFirstInternal() {
        iterator.seekToFirst();
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        iterator.seek(targetKey);
    }

    @Override
    protected Entry<Slice, Slice> getNextElement() {
        while(iterator.hasNext()){
            Entry<Slice, Slice> entry = iterator.next();
            if(isValidProId(entry)){
                return entry;
            }
        }
        return null;
    }

    private boolean isValidProId(Entry<Slice, Slice> entry){
        InternalKey key = new InternalKey(entry.getKey());
        return idSet.contains(key.getPropertyId());
    }
}
