package edu.buaa.act.temporal.impl.memlog;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import org.act.temporalProperty.util.ByteBufferSupport;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by song on 2018-01-01.
 */
public class MemLogWriter
{
    private MappedByteBuffer out;
    private boolean blockBegan = false;
    private ByteArrayDataOutput buf = ByteStreams.newDataOutput();

    public MemLogWriter(MappedByteBuffer out) throws IOException
    {
        this.out = out;
    }

    public void beginBlock()
    {
        if(!this.blockBegan)
        {
            this.blockBegan = true;
            buf = ByteStreams.newDataOutput();
        }else{
            throw new TPSRuntimeException("block not closed");
        }
    }

    public void append(long entityId, TimePoint time, ValueAtTime value)
    {
        if(blockBegan)
        {
            buf.writeLong(entityId);
            buf.write(time.encode());
            buf.write(value.encode());
        }else{
            throw new TPSRuntimeException("should call beginBlock() first");
        }
    }

    public void endBlock()
    {
        byte[] blockContent = buf.toByteArray();
        out.putInt(blockContent.length);
        out.put(blockContent);
        out.force();
    }


}
