package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.Slice;

import java.util.Map;
import java.util.Map.Entry;

/**
 * If an entity only contains one entry and the entry is INVALID, then remove such entries.
 * Created by song on 2018-03-28.
 */
public class InvalidEntityFilterIterator extends PairViewFilterIterator<Entry<Slice,Slice>> implements SeekingIterator<Slice,Slice>{
    private boolean lastTwoIDEqual=false;// true if entry[cur-1].id == entry[cur].id

    public InvalidEntityFilterIterator(SeekingIterator<Slice,Slice> in) {
        super(in);
    }

    @Override
    protected boolean shouldReturnFirst(Entry<Slice, Slice> cur, Entry<Slice, Slice> next) {
        InternalKey curKey = new InternalKey(cur.getKey());
        if(next!=null) {
            InternalKey nextKey = new InternalKey(next.getKey());
            if (nextKey.getId().equals(curKey.getId())) {
                lastTwoIDEqual = true;
                return true;
            } else {
                if (!lastTwoIDEqual && curKey.getValueType() == ValueType.INVALID) {
                    lastTwoIDEqual = false;
                    return false;
                } else {
                    lastTwoIDEqual = false;
                    return true;
                }
            }
        }else{
            if(!lastTwoIDEqual && curKey.getValueType() == ValueType.INVALID){
                return false;
            }else{
                return true;
            }
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
