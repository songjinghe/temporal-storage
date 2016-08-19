
package org.act.dynproperty.impl;

import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.act.dynproperty.table.UserComparator;

/**
 * 用于比较两个InternalKey的顺序
 *
 */
public class InternalKeyComparator
        implements Comparator<InternalKey>
{
    private final UserComparator userComparator;

    public InternalKeyComparator(UserComparator userComparator)
    {
        this.userComparator = userComparator;
    }

    public UserComparator getUserComparator()
    {
        return userComparator;
    }

    public String name()
    {
        return this.userComparator.name();
    }

    /**
     * 比较两个InternalKey的顺序，首先对两个的id进行比较，如果是同一个动态属性，则对时间进行比较。
     */
    @Override
    public int compare(InternalKey left, InternalKey right)
    {
        int result = userComparator.compare(left.getId(), right.getId());
        if (result != 0) {
            return result;
        }

        return Longs.compare(left.getStartTime(), right.getStartTime());
    }

    /**
     * Returns {@code true} if each element in {@code iterable} after the first is
     * greater than or equal to the element that preceded it, according to this
     * ordering. Note that this is always true when the iterable has fewer than
     * two elements.
     */
    public boolean isOrdered(InternalKey... keys)
    {
        return isOrdered(Arrays.asList(keys));
    }

    /**
     * Returns {@code true} if each element in {@code iterable} after the first is
     * greater than or equal to the element that preceded it, according to this
     * ordering. Note that this is always true when the iterable has fewer than
     * two elements.
     */
    public boolean isOrdered(Iterable<InternalKey> keys)
    {
        Iterator<InternalKey> iterator = keys.iterator();
        if (!iterator.hasNext()) {
            return true;
        }

        InternalKey previous = iterator.next();
        while (iterator.hasNext()) {
            InternalKey next = iterator.next();
            if (compare(previous, next) > 0) {
                return false;
            }
            previous = next;
        }
        return true;
    }
}
