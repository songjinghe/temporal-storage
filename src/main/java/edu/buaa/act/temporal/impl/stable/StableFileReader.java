package edu.buaa.act.temporal.impl.stable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.impl.table.ETEntry;
import edu.buaa.act.temporal.impl.iterator.SearchableIterator;
import edu.buaa.act.temporal.impl.table.ETVEntry;
import edu.buaa.act.temporal.impl.unstable.UnStableFileReader;

import java.nio.MappedByteBuffer;

/**
 * Created by song on 2018-01-03.
 */
public class StableFileReader extends UnStableFileReader
{
    public StableFileReader(MappedByteBuffer fileMap)
    {
        super(fileMap);
    }
}
