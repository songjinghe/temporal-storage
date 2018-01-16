package edu.buaa.act.temporal.impl.table;

import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.helper.Slice;
import edu.buaa.act.temporal.impl.iterator.SearchableIterator;

/**
 * Created by song on 17-12-8.
 */
public class Block extends Slice
{

    private BlockWriter writer;

    public Block(int length)
    {
        super(length);
    }

    public BlockWriter getWriter()
    {
        return writer;
    }
}
