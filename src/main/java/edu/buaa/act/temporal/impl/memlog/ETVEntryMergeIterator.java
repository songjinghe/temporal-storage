package edu.buaa.act.temporal.impl.memlog;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.impl.table.ETVEntry;

/**
 * Created by song on 2018-01-02.
 *
 * compare entity id and time,
 * values are in raw format.
 *
 * used to merge iterator from different block of memlog file.
 * also can be used across memlog files.
 */
public class ETVEntryMergeIterator extends AbstractIterator<ETVEntry> implements PeekingIterator<ETVEntry>
{
    private final PeekingIterator<ETVEntry> newIter;
    private final PeekingIterator<ETVEntry> oldIter;

    public ETVEntryMergeIterator(PeekingIterator<ETVEntry> oldEntryIter, PeekingIterator<ETVEntry> newEntryIter)
    {
        this.oldIter = oldEntryIter;
        this.newIter = newEntryIter;
    }

    @Override
    protected ETVEntry computeNext()
    {
        while (newIter.hasNext()) // remove head unknown values in newIter
        {
            if (newIter.peek().getValue().isUnknown())
            {
                newIter.next(); // continue next loop;
            } else
            {
                break;
            }
        }

        if (newIter.hasNext() && oldIter.hasNext())
        {
            ETVEntry mem = newIter.peek();
            ETVEntry disk = oldIter.peek();

            if (mem.getTime().compareTo(disk.getTime()) <= 0)
            {
                newIter.next();
                if (newIter.hasNext())
                {
                    pollUntil(oldIter, newIter.peek().getTime());
                } else
                {
                    pollUntil(oldIter, TimePoint.NOW);
                }
                return mem;
            } else
            {
                oldIter.next();
                return disk;
            }
        } else if (newIter.hasNext()) // diskIter run out
        {
            return newIter.next();
        } else if (oldIter.hasNext()) // memIter run out
        {
            return oldIter.next();
        } else // both ran out
        {
            return endOfData();
        }
    }

    private void pollUntil(PeekingIterator<ETVEntry> iter, TimePoint time)
    {
        while (iter.hasNext())
        {
            if (iter.peek().getTime().compareTo(time) < 0)
            {
                iter.next();
            } else
            {
                return;
            }
        }
    }
}
