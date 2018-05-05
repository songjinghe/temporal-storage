package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by song on 2018-01-24.
 */
public class EPEntryIterator extends AbstractSearchableIterator {

    private final SearchableIterator iter;
    private final Slice id;

    public EPEntryIterator(Slice entityPropertyId, SearchableIterator iterator){
        super(iterator);
        this.id = entityPropertyId;
        this.iter = iterator;
        this.seekToFirst();
    }

    @Override
    public void seekToFirst() {
        InternalKey earliestKey = new InternalKey(id, 0);
        iter.seek(earliestKey);
    }

    private boolean validId(InternalEntry entry) {
        return entry.getKey().getId().equals(id);
    }

    @Override
    protected InternalEntry computeNext() {
        while(iter.hasNext()){
            InternalEntry entry = iter.next();
            if(validId(entry)){
                return entry;
            }
        }
        return endOfData();
    }
}
