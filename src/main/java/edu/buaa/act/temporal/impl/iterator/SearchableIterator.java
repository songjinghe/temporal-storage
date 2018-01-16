package edu.buaa.act.temporal.impl.iterator;

import com.google.common.collect.PeekingIterator;

import java.util.Map;

/**
 * Created by song on 2018-01-02.
 */
public interface SearchableIterator<K>
{
    /**
     * Repositions the iterator so the beginning of this block.
     */
    void seekToFirst();

    /**
     * Repositions the iterator so the key of the next BlockElement returned greater than or equal to the specified targetKey.
     */
    void seekFloor(K targetKey);
}
