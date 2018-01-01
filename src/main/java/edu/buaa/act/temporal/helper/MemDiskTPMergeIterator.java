package edu.buaa.act.temporal.helper;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.TimePointValueEntry;

import java.util.NoSuchElementException;

/**
 * Created by song on 17-12-28.
 *
 * Merge TimePointValueEntry from iterators of MemTable and Disk;
 *
 * the input `memIter` in this class can have `ValueAtTime.Unknown` values
 * but the output iterator do not contains `ValueAtTime.Unknown` values
 */

public class MemDiskTPMergeIterator implements PeekingIterator<TimePointValueEntry>
{
    private final PeekingIterator<TimePointValueEntry> diskIter;
    private final PeekingIterator<TimePointValueEntry> memIter;

    private TimePointValueEntry peeked;

    public MemDiskTPMergeIterator(PeekingIterator<TimePointValueEntry> memIter, PeekingIterator<TimePointValueEntry> diskIter)
    {
        this.memIter = memIter;
        this.diskIter = diskIter;
        this.peeked = findNext();
    }

    @Override
    public TimePointValueEntry peek()
    {
        if (this.peeked != null) return peeked;
        else throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext()
    {
        return this.peeked != null;
    }

    @Override
    public TimePointValueEntry next()
    {
        TimePointValueEntry tmp = this.peeked;
        this.peeked = findNext();
        return tmp;
    }

    private TimePointValueEntry findNext()
    {
        TimePointValueEntry mem, disk;

        while (memIter.hasNext())
        {
            if (memIter.peek().getValue().isUnknown())
            {
                memIter.next(); // continue next loop;
            } else
            {
                break;
            }
        }

        if (memIter.hasNext() && diskIter.hasNext())
        {
            mem = memIter.peek();
            disk = diskIter.peek();

            if (mem.getTime().compareTo(disk.getTime()) <= 0)
            {
                memIter.next();
                if (memIter.hasNext())
                {
                    removeBefore(diskIter, memIter.peek().getTime());
                } else
                {
                    removeBefore(diskIter, TimePoint.NOW);
                }
                return mem;
            } else
            {
                diskIter.next();
                return disk;
            }
        } else if (memIter.hasNext()) // diskIter run out
        {
            return memIter.next();
        } else if (diskIter.hasNext()) // memIter run out
        {
            return diskIter.next();
        } else // both ran out
        {
            return null;
        }
    }

    private void removeBefore(PeekingIterator<TimePointValueEntry> iter, TimePoint time)
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

    @Override
    public void remove()
    {
        throw new TPSRuntimeException("operation not supported");
    }
}
