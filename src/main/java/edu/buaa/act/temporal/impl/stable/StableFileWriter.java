package edu.buaa.act.temporal.impl.stable;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TemporalPropertyStorage;
import edu.buaa.act.temporal.impl.table.ETVEntry;
import edu.buaa.act.temporal.impl.unstable.ETVEncodeIterator;
import edu.buaa.act.temporal.impl.unstable.UnStableFileWriter;
import edu.buaa.act.temporal.io.PackToBlockIterator;

import java.nio.MappedByteBuffer;

/**
 * Created by song on 2018-01-03.
 */
public class StableFileWriter extends UnStableFileWriter
{
    public StableFileWriter(MappedByteBuffer map)
    {
        super(map);
    }
}
