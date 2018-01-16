package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.helper.Slice;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.nio.MappedByteBuffer;
import java.security.MessageDigest;

import static edu.buaa.act.temporal.io.TemporalDataFile.FILE_MAGIC_NUMBER;
import static edu.buaa.act.temporal.io.TemporalDataFile.FILE_VERSION;

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
        int placeHolder = putHeader();

        VarLenValueFilePackIterator blockPackIter = new VarLenValueFilePackIterator(iterator, 1024*1024, 4*1024);

        int dataStart = out.position();

        while(blockPackIter.hasNext())
        {
            Slice block = blockPackIter.next();
            out.put(block.array());
        }

        int dataLen = out.position()-dataStart;

        out.putInt(placeHolder, dataLen);

        MessageDigest md5 = blockPackIter.getMD5();
        byte[] indexSegment = blockPackIter.getIndex().array();
        md5.update(indexSegment);

        out.putInt(placeHolder+4, indexSegment.length);

        out.put(indexSegment);
        out.put(md5.digest());

        out.force();
    }

    private int putHeader()
    {
        out.putLong(FILE_MAGIC_NUMBER);
        out.putInt(FILE_VERSION);
        int placeHolderStart = out.position();
        out.putInt(0); //placeholder for data length
        out.putInt(0); //placeholder for index length
        return placeHolderStart;
    }
}
