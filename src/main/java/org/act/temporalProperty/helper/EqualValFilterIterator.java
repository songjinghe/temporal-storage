package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * This iterator merge two (or more) next entries whose values are equal (entityID, propertyId are also equal, of course)
 * only the first (earlier) entry is retained.
 * Created by song on 2018-03-28.
 */
public class EqualValFilterIterator extends AbstractIterator<Entry<Slice,Slice>> implements SeekingIterator<Slice,Slice> {

    private final SeekingIterator<Slice, Slice> in;
    private Entry<Slice, Slice> cur;

    public EqualValFilterIterator(SeekingIterator<Slice,Slice> in)
    {
        this.in = in;
        if(in.hasNext()) cur = in.next();
    }

    @Override
    protected Entry<Slice, Slice> computeNext() {
        if(cur !=null) {
            while (in.hasNext()) {
                Entry<Slice, Slice> next = in.next();
                InternalKey preKey = new InternalKey(cur.getKey());
                InternalKey curKey = new InternalKey(next.getKey());
                if (preKey.getId().equals(curKey.getId()) && cur.getValue().equals(next.getValue())) {
                    //do nothing, continue loop
                } else {
                    Entry<Slice, Slice> tmp = cur;
                    cur = next;
                    return tmp;
                }
            }
            Entry<Slice, Slice> tmp = cur;
            cur = null;
            return tmp;
        }else{
            return endOfData();
        }
    }

    @Override
    public void seekToFirst() {
        in.seekToFirst();
    }

    @Override
    public void seek(Slice targetKey) {
        in.seek(targetKey);
    }
}
