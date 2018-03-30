package org.act.temporalProperty.index;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.*;
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
public class PropertyFilterIterator extends AbstractSearchableIterator implements SearchableIterator{

    private final Set<Integer> idSet;

    public PropertyFilterIterator(List<Integer> proIds, SearchableIterator iterator){
        super(iterator);
        this.idSet = new HashSet<>(proIds);
    }

    private boolean isValidProId(InternalEntry entry){
        return idSet.contains(entry.getKey().getPropertyId());
    }

    @Override
    protected InternalEntry computeNext() {
        while(iterator.hasNext()){
            InternalEntry entry = iterator.next();
            if(isValidProId(entry)){
                return entry;
            }
        }
        return endOfData();
    }
}
