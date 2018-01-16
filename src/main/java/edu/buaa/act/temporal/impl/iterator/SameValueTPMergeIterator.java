package edu.buaa.act.temporal.impl.iterator;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.TimePointValueEntry;

import java.util.NoSuchElementException;

/**
 * Created by song on 17-12-28.
 *
 * Remove TimePoints whichMerge TimePointValueEntry from iterators of MemTable and Disk;
 */
public class SameValueTPMergeIterator implements PeekingIterator<TimePointValueEntry>
{
    private final PeekingIterator<TimePointValueEntry> iterator;

    private TimePointValueEntry peeked;

    public SameValueTPMergeIterator(PeekingIterator<TimePointValueEntry> iterator)
    {
        this.iterator = iterator;
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
        if(peeked==null)
        {
            if(iterator.hasNext())
            {
                return iterator.next();
            }else{
                return null;
            }
        }else
        {
            while(iterator.hasNext())
            {
                if(!iterator.peek().getValue().equals(peeked.getValue())){
                    return iterator.next();
                }else{
                    iterator.next();
                }
            }
            return null;
        }
    }

    @Override
    public void remove()
    {
        throw new TPSRuntimeException("operation not supported");
    }
}
