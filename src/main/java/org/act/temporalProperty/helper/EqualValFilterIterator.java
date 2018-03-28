package org.act.temporalProperty.helper;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * This iterator merge two (or more) next entries whose values are equal (entityID, propertyId are also equal, of course)
 * only the first (earlier) entry is retained.
 * Created by song on 2018-03-28.
 */
public class EqualValFilterIterator extends PairViewFilterByPreIterator<Entry<Slice,Slice>> implements SeekingIterator<Slice,Slice> {

    public EqualValFilterIterator(SeekingIterator<Slice,Slice> in) {
        super(in);
    }

    @Override
    protected boolean shouldReturnSecond(Entry<Slice, Slice> lastReturned, Entry<Slice, Slice> cur) {
        if(lastReturned!=null){
            InternalKey preKey = new InternalKey(lastReturned.getKey());
            InternalKey curKey = new InternalKey(cur.getKey());
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
    public void seek(Slice targetKey) {
        throw new UnsupportedOperationException();
    }
}
