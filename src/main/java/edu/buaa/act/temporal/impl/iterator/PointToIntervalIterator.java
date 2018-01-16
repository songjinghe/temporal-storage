package edu.buaa.act.temporal.impl.iterator;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimeInterval;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.TimeValueEntry;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.TimePointValueEntry;

import java.util.NoSuchElementException;

/**
 * Created by song on 17-12-28.
 */
public class PointToIntervalIterator implements PeekingIterator<TimeValueEntry>
{
    private PeekingIterator<TimePointValueEntry> tpIter;
    private TimeValueEntry peeked;

    public PointToIntervalIterator(PeekingIterator<TimePointValueEntry> timePointValueIter)
    {
        this.tpIter = timePointValueIter;
        this.peeked = findNext();
    }

    @Override
    public TimeValueEntry peek()
    {
        if (peeked != null)
        {
            return peeked;
        } else
        {
            throw new NoSuchElementException();
        }
    }

    @Override
    public boolean hasNext()
    {
        return peeked != null;
    }

    @Override
    public TimeValueEntry next()
    {
        if (peeked != null)
        {
            TimeValueEntry tv = this.peeked;
            this.peeked = findNext();
            return tv;
        } else {
            throw new NoSuchElementException();
        }
    }

    private TimeValueEntry findNext()
    {
        if(tpIter.hasNext())
        {
            TimePointValueEntry tpv1 = tpIter.next();
            if(tpIter.hasNext())
            {
                TimePointValueEntry tpv2 = tpIter.peek();
                if(tpv2.getTime().hasPre())
                {
                    return new TimeValueEntry(new TimeInterval(tpv1.getTime(), tpv2.getTime().pre()), tpv1.getValue());
                }else{
                    throw new TPSRuntimeException("SNH: time not have pre");
                }
            }else{
                return new TimeValueEntry(new TimeInterval(tpv1.getTime(), TimePoint.NOW), tpv1.getValue());
            }
        }else{
            return null;
        }
    }

    @Override
    public void remove()
    {
        throw new TPSRuntimeException("operation not supported");
    }
}
