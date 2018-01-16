package edu.buaa.act.temporal.impl.memlog;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.nio.MappedByteBuffer;

/**
 * Created by song on 2018-01-02.
 */
public class MemLogFileBlockIterator extends AbstractIterator<ETVEntry> implements PeekingIterator<ETVEntry>
{
    private final MappedByteBuffer in;
    private final int start;
    private final int len;
    private int cur;

    public MemLogFileBlockIterator(MappedByteBuffer in, int blockStart, int blockLength)
    {
        this.in = in;
        this.start = blockStart;
        this.len = blockLength;
        this.cur = start;
    }

    @Override
    protected ETVEntry computeNext()
    {
        if(cur>start+len){
            return endOfData();
        }else
        {
            in.position(cur);
            long entityId = in.getLong();
            TimePoint t = TimePoint.IO.decode(in);
            ValueAtTime v = ValueAtTime.decode(in);
            cur = in.position();
            return new ETVEntry(entityId, t, v);
        }
    }
}
