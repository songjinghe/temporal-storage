package edu.buaa.act.temporal.impl.memlog;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.nio.MappedByteBuffer;

/**
 * Created by song on 2018-01-02.
 */
public class MemLogFileIteratorReader extends AbstractIterator<ETVEntry> implements PeekingIterator<ETVEntry>
{
    private final MappedByteBuffer in;
    private PeekingIterator<ETVEntry> blockIterators;

    public MemLogFileIteratorReader(MappedByteBuffer buffer)
    {
        this.in = buffer;
        this.in.flip();
        initRead();
    }

    @Override
    protected ETVEntry computeNext()
    {
        if (blockIterators.hasNext()) return blockIterators.next();
        else return endOfData();
    }

    private void initRead()
    {
        this.in.position(0);
        long createTime = in.getLong();
        long lastModifiedTime = in.getLong();
        int  blockCount = in.getInt();
        this.blockIterators = recursiveIter(blockCount); //TODO: non-recursive iterator when blockCount too large.
    }

    private PeekingIterator<ETVEntry> recursiveIter(int blockCount)
    {
        PeekingIterator<ETVEntry> cur = null;

        for (int i = 0; i < blockCount; i++)
        {
            int blockLength = in.getInt();
            int blockStart = in.position();
            PeekingIterator<ETVEntry> iter = new MemLogFileBlockIterator(in, blockStart, blockLength);
            if (i == 0)
            {
                cur = iter;
            } else
            {
                cur = new ETVEntryMergeIterator(cur, iter);
            }

            if (in.remaining() >= blockLength)// jump to next block
            {
                in.position(blockStart + blockLength);
            } else
            {
                return cur;
            }
        }
        return cur;
    }
}