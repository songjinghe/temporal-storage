package org.act.temporalProperty.helper;

import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * This iterator merge two (or more) next entries whose values are equal (entityID, propertyId are also equal, of course)
 * only the first (earlier) entry is retained.
 * Created by song on 2018-03-28.
 */
public class EqualValFilterIterator extends PairViewFilterByPreIterator<InternalEntry> implements SearchableIterator {

    public EqualValFilterIterator(SearchableIterator in) {
        super(in);
    }

    @Override
    protected boolean shouldReturnSecond(InternalEntry lastReturned, InternalEntry cur) {
        if(lastReturned!=null){
            InternalKey preKey = lastReturned.getKey();
            InternalKey curKey = cur.getKey();
            if (curKey.getId().equals(preKey.getId()) && cur.getValue().equals(lastReturned.getValue())) {
                return false;
            } else {
                return true;
            }
        }else{
            return true;
        }
    }

    @Override
    public void seekToFirst() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(InternalKey targetKey) {
        throw new UnsupportedOperationException();
    }
}
