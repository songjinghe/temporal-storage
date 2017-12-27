package edu.buaa.act.temporal;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Created by song on 17-12-5.
 */
public interface ValueAtTime<T extends ValueAtTime<T>> extends Comparable<T>, Serializable
{
    int length();
    byte[] encode();
    boolean comparable();


    ValueAtTime Invalid  = new ValueAtTime()
    {
        @Override
        public int compareTo(Object o)
        {
            return 0;
        }

        @Override
        public int length()
        {
            return 0;
        }

        @Override
        public byte[] encode()
        {
            return new byte[0];
        }

        @Override
        public boolean comparable()
        {
            return false;
        }
    };

    static ValueAtTime decode(ByteBuffer in, int vLen)
    {
        return null;
    }
}
