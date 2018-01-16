package edu.buaa.act.temporal;

import com.google.common.primitives.Ints;
import edu.buaa.act.temporal.helper.IOCoder;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by song on 17-12-29.
 *
 */
public class IntegerValue implements ValueAtTime<Integer>
{
    private int value;

    @Override
    public int length()
    {
        return 4;
    }

    @Override
    public IOCoder<Integer> io()
    {
        return coder;
    }

    @Override
    public boolean comparable()
    {
        return true;
    }

    @Override
    public Integer value()
    {
        return value;
    }

    @Override
    public void set(Integer value)
    {
        this.value = value;
    }

    @Override
    public boolean isInvalid()
    {
        return false;
    }

    @Override
    public boolean isUnknown()
    {
        return false;
    }

    @Override
    public int compareTo(Integer o)
    {
        return Integer.compare(value, o);
    }

    private static IOCoder<Integer> coder = new IOCoder<Integer>()
    {
        @Override
        public int rawSize()
        {
            return 4;
        }

        @Override
        public Integer decode(byte[] src)
        {
            return Ints.fromByteArray(src);
        }

        @Override
        public Integer decode(DataInput in) throws IOException
        {
            return in.readInt();
        }

        @Override
        public Integer decode(ByteBuffer in)
        {
            return in.getInt();
        }

        @Override
        public byte[] encode(Integer integer)
        {
            return Ints.toByteArray(integer);
        }
    };
}
