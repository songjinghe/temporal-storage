package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.helper.Slice;
import edu.buaa.act.temporal.impl.table.Block;
import edu.buaa.act.temporal.impl.table.BlockWriter;
import edu.buaa.act.temporal.impl.table.ETVEntry;

/**
 * Created by song on 2018-01-04.
 *
 * write and encode data.
 * build inner block index.
 *
 */
public class ETV2BlockPackingIterator extends AbstractIterator<Block> implements PeekingIterator<Block>
{
    private final int blockSize = 4*1024*1024;
    private final int minCrossBlockSize = 400;//bytes
    private final PeekingIterator<ETVEntry> in;
    private Slice valueToWriteFirst;


    public ETV2BlockPackingIterator(PeekingIterator<ETVEntry> iterator)
    {
        this.in = iterator;
    }

    @Override
    protected Block computeNext()
    {
        Block block = new Block(blockSize);
        BlockWriter w = block.getWriter();

        if(valueToWriteFirst==null)
        {
            if(in.hasNext())
            {
                return addToBlock(block, w);
            }else{
                return endOfData();
            }
        }else{
            w.addPreValue(valueToWriteFirst);

            if(!valueToWriteFirst.hasRemaining()) valueToWriteFirst = null;

            return addToBlock(block, w);
        }
    }

    private Block addToBlock(Block block, BlockWriter w)
    {
        while(in.hasNext() && w.hasRemaining())
        {
            ETVEntry etvEntry = in.peek();
            int remains = w.remainingAfterAdd(etvEntry);
            if(remains>=0)
            {
                w.addETV(etvEntry);
                in.next();
            }
            else
            {
                remains = 0-remains;
                int vLen = etvEntry.getValue().length();
                if( (remains > minCrossBlockSize && (vLen / remains < 3)) ||
                        ( vLen > blockSize * 0.8 ))
                {
                    valueToWriteFirst = w.addETVPart(etvEntry);
                    in.next();
                    w.finish();
                    return block;
                }else
                {
                    w.finish();
                    return block;
                }
            }
        }
        w.finish();
        return block;
    }
}























