package org.act.temporalProperty.impl;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * Created by song on 2018-03-29.
 */
public abstract class AbstractSearchableIterator extends AbstractIterator<InternalEntry> implements SearchableIterator {

    private final SearchableIterator in;

    public AbstractSearchableIterator(SearchableIterator in){
        this.in = in;
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
