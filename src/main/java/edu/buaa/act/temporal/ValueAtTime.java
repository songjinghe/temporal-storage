package edu.buaa.act.temporal;

import java.nio.ByteBuffer;

/**
 * Created by song on 17-12-5.
 */
public interface ValueAtTime extends Comparable<ValueAtTime>
{
    boolean comparable();
    boolean isInvalid();
    boolean isUnknown();
    byte[]  encode();
    // content length of value, not raw length on disk.
    int length();

    int EntityLen = 8;
    // represent this interval (and its corresponding value) is deleted by user.
    ValueAtTime Invalid  = new ValueAtTime()
    {
        @Override
        public int compareTo(ValueAtTime o)
        {
            if(o==Invalid) return 0;
            else return -1;
        }

        @Override
        public boolean comparable()
        {
            return false;
        }

        @Override
        public boolean isInvalid()
        {
            return true;
        }

        @Override
        public boolean isUnknown()
        {
            return false;
        }

        @Override
        public byte[] encode()
        {
            return new byte[]{-1};
        }

        @Override
        public int length()
        {
            return 0;
        }
    };

    // represent you should search disk for more information.
    // can only occur in memtable and memlog, not in unstable and stable files.
    ValueAtTime Unknown = new ValueAtTime()
    {
        @Override
        public int compareTo(ValueAtTime o)
        {
            if(o==Unknown) return 0;
            else return 1;
        }

        @Override
        public boolean comparable()
        {
            return false;
        }

        @Override
        public boolean isInvalid()
        {
            return false;
        }

        @Override
        public boolean isUnknown()
        {
            return true;
        }

        @Override
        public byte[] encode()
        {
            return new byte[]{-2};
        }

        @Override
        public int length()
        {
            return 0;
        }
    };

    static int remain(int raw)
    {
        if(raw<0) return 0;
        else return raw;
    }

    static ValueAtTime decode(ByteBuffer in)
    {
        return null;
    }

}
