package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.PeekingIterator;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import edu.buaa.act.temporal.TemporalPropertyStorage;
import edu.buaa.act.temporal.impl.table.ETVEntry;
import edu.buaa.act.temporal.io.PackToBlockIterator;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

/**
 * Created by song on 2018-01-02.
 */
public class UnStableFileWriter
{
    private final MappedByteBuffer out;

    public UnStableFileWriter(MappedByteBuffer map)
    {
        this.out = map;
    }

    public void consumeAndBuild(PeekingIterator<ETVEntry> iterator)
    {
        putHeader();

        PeekingIterator<byte[]> contentIter = new ETVEncodeIterator(iterator, 4*1024*1024);
        PeekingIterator<byte[]> blockPackIter = new PackToBlockIterator(contentIter, 4*1024);

        while(blockPackIter.hasNext())
        {
            byte[] block = blockPackIter.next();
            out.put(block);
        }
    }

    private void putHeader()
    {
        String header = TemporalPropertyStorage.version + " UNSTABLE_FILE";
        out.putInt(header.length());
        out.put(header.getBytes());
    }
}
