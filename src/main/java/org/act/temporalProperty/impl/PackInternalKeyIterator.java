package org.act.temporalProperty.impl;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * Created by song on 2018-03-29.
 */
public class PackInternalKeyIterator extends AbstractIterator<InternalEntry> implements SearchableIterator {

    private final SeekingIterator<Slice, Slice> in;

    public PackInternalKeyIterator(SeekingIterator<Slice, Slice> in){
        this.in = in;
    }

    @Override
    protected InternalEntry computeNext() {
        if(in.hasNext()){
            Entry<Slice, Slice> tmp = in.next();
            return new InternalEntry(new InternalKey(tmp.getKey()), tmp.getValue());
        }else {
            return endOfData();
        }
    }

    @Override
    public void seekToFirst() {
        in.seekToFirst();
    }

    @Override
    public void seek(InternalKey targetKey) {
        in.seek(targetKey.encode());
    }
}
