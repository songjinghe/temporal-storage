package edu.buaa.act.temporal.impl.index;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimeValueEntry;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.nio.MappedByteBuffer;

/**
 * Created by song on 2018-01-09.
 */
public class IndexFileWriter
{

    private final MappedByteBuffer in;

    public IndexFileWriter(MappedByteBuffer map)
    {
        this.in = map;
    }

    public void consumeAndWrite(PeekingIterator<ETVEntry> iterator)
    {

        PeekingIterator<TimeValueEntry> iter = new SingleValueIndexDataEntryFactory(iterator);
    }




}
