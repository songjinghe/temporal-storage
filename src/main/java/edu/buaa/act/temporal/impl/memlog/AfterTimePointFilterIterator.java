package edu.buaa.act.temporal.impl.memlog;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.impl.table.ETVEntry;

/**
 * Created by song on 2018-01-05.
 */
public class AfterTimePointFilterIterator implements PeekingIterator<ETVEntry>
{
    private final PeekingIterator<ETVEntry> mem;
    private final TimePoint stMaxTime;

    public AfterTimePointFilterIterator(PeekingIterator<ETVEntry> m, TimePoint stMaxTime)
    {
        this.mem = m;
        this.stMaxTime = stMaxTime;
    }

    @Override
    public ETVEntry peek()
    {
        //return null;
    }

    @Override
    public boolean hasNext()
    {
        //return false;
    }

    @Override
    public ETVEntry next()
    {
        //return null;
    }

    @Override
    public void remove()
    {
        //
    }
}
