package org.act.temporalProperty.impl;

import com.google.common.collect.AbstractIterator;

/**
 * Created by song on 2018-03-29.
 */
public abstract class AbstractSearchableIterator extends AbstractIterator<InternalEntry> implements SearchableIterator {

    protected final SearchableIterator iterator;

    public AbstractSearchableIterator(SearchableIterator in){
        this.iterator = in;
    }

    @Override
    public void seekToFirst() {
        iterator.seekToFirst();
    }

    @Override
    public void seek(InternalKey targetKey) {
        iterator.seek(targetKey);
    }
}
