package org.act.temporalProperty.index;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;

import java.util.Set;

/**
 * Created by song on 2018-04-05.
 */
public class EntityFilterIterator extends AbstractIterator<InternalEntry> implements SearchableIterator {

    private final Set<Long> entityIdSet;
    private final SearchableIterator in;

    public EntityFilterIterator(Set<Long> entityIds, SearchableIterator in) {
        entityIdSet = entityIds;
        this.in = in;
    }

    @Override
    protected InternalEntry computeNext() {
        while(in.hasNext()){
            if(entityIdSet.contains(in.peek().getKey().getEntityId())){
                return in.next();
            }else{
                in.next();
            }
        }
        return endOfData();
    }

    @Override
    public void seekToFirst() {
        in.seekToFirst();
    }

    @Override
    public void seek(InternalKey targetKey) {
        in.seek(targetKey);
    }
}
