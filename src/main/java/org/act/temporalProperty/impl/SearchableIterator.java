package org.act.temporalProperty.impl;

import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-03-29.
 */
public interface SearchableIterator extends PeekingIterator<InternalEntry> {
    /**
     * Repositions the iterator so the beginning of this block.
     */
    void seekToFirst();

    /**
     * edit description by sjh @2018-01-25
     * Origin description:
     * Repositions the iterator so the key of the next BlockElement returned greater than or equal to the specified targetKey.
     * New description:  (similar to FLOOR operator)
     * if data key is 1, 3, 5, then
     * |seek | peek() and next() returns |
     * |  0  |  1  | ? not for sure
     * |  1  |  1  |
     * |  2  |  1  |
     * |  3  |  3  |
     * |  4  |  3  |
     * |  5  |  5  |
     * |  6  |  5  |
     * IMPORTANT NOTE:
     * the behavior of peek() and next() is defined by implementations. For example, a implementation may
     * only return entries whose key > 2, thus seek(0), seek(1), seek(2), seek(3) have the same effect.
     */
    void seek(InternalKey targetKey);
}
