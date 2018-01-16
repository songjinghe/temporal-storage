package edu.buaa.act.temporal.helper;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * Created by song on 2018-01-04.
 */
public class Slice implements Comparable<Slice>
{

    private final ByteBuffer data;

    public Slice(int length)
    {
        data = ByteBuffer.allocate(length);
//        data.order(ByteOrder.LITTLE_ENDIAN);
    }

    public Slice(byte[] data)
    {
        Preconditions.checkNotNull(data, "array is null");
        this.data = ByteBuffer.wrap(data);
//        this.data.order(ByteOrder.LITTLE_ENDIAN);
    }

    public Slice(byte[] data, int offset, int length)
    {
        Preconditions.checkNotNull(data, "array is null");
        this.data = ByteBuffer.wrap(data, offset, length);
//        this.data.order(ByteOrder.LITTLE_ENDIAN);
    }
    
    public Slice(ByteBuffer data)
    {
        this.data = data.duplicate();
    }

    public Slice(ByteBuffer data, int len)
    {
        this.data = data.duplicate();
        this.data.limit(data.arrayOffset()+len);
    }

    public Slice slice()
    {
        return new Slice(this.data.slice()
//                .order(ByteOrder.LITTLE_ENDIAN)
        );
    }

    public byte get()
    {
        return data.get();
    }

    public Slice put(byte b)
    {
        data.put(b);
        return this;
    }

    public byte get(int index)
    {
        return data.get(index);
    }

    public Slice put(int index, byte b)
    {
        data.put(index, b);
        return this;
    }

    public char getChar()
    {
        return data.getChar();
    }

    public Slice putChar(char value)
    {
        data.putChar(value);
        return this;
    }

    public char getChar(int index)
    {
        return data.getChar(index);
    }

    public Slice putChar(int index, char value)
    {
        data.putChar(index,value);
        return this;
    }

    
    public short getShort()
    {
        return data.getShort();
    }

    
    public Slice putShort(short value)
    {
        data.putShort(value);
        return this;
    }

    
    public short getShort(int index)
    {
        return data.getShort(index);
    }

    
    public Slice putShort(int index, short value)
    {
        data.putShort(index, value);
        return this;
    }

    
    public int getInt()
    {
        return data.getInt();
    }

    
    public Slice putInt(int value)
    {
        data.putInt(value);
        return this;
    }

    
    public int getInt(int index)
    {
        return data.getInt(index);
    }

    
    public Slice putInt(int index, int value)
    {
        data.putInt(index, value);
        return this;
    }

    
    public long getLong()
    {
        return data.getLong();
    }

    
    public Slice putLong(long value)
    {
        data.putLong(value);
        return this;
    }

    
    public long getLong(int index)
    {
        return data.getInt(index);
    }

    
    public Slice putLong(int index, long value)
    {
        data.putLong(index, value);
        return this;
    }

    
    public float getFloat()
    {
        return data.getFloat();
    }

    
    public Slice putFloat(float value)
    {
        data.putFloat(value);
        return this;
    }

    
    public float getFloat(int index)
    {
        return data.getFloat(index);
    }

    
    public Slice putFloat(int index, float value)
    {
        data.putFloat(index, value);
        return this;
    }

    
    public double getDouble()
    {
        return data.getDouble();
    }

    
    public Slice putDouble(double value)
    {
        data.putDouble(value);
        return this;
    }

    
    public double getDouble(int index)
    {
        return data.getDouble(index);
    }

    
    public Slice putDouble(int index, double value)
    {
        data.putDouble(index, value);
        return this;
    }

    public int position(){
        return data.position();
    }

    public int remaining(){
        return data.remaining();
    }

    public boolean hasRemaining(){
        return data.hasRemaining();
    }

    public Slice position(int newPos){
        data.position(newPos);
        return this;
    }

    public int arrayOffset(){
        return data.arrayOffset();
    }

    public int capacity(){
        return data.capacity();
    }

    public byte[] array(){
        return data.array();
    }

    @Override
    public int compareTo(Slice o)
    {
        return 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Slice slice = (Slice) o;

        // do lengths match
        if (capacity() != slice.capacity()) {
            return false;
        }

        // if arrays have same base offset, some optimizations can be taken...
        if (offset == slice.offset && data == slice.data) {
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (data[offset + i] != slice.data[slice.offset + i]) {
                return false;
            }
        }
        return true;
    }

    public Slice put(byte[] src)
    {
        data.put(src);
        return this;
    }

    public Slice clear()
    {
        data.clear();
        return this;
    }

    public int compareTo(ByteBuffer in, int pos, int len)
    {
        int result = Integer.compare(len, this.data.limit());
        if(result!=0){
            return result;
        }else
        {
            for (int i = this.data.arrayOffset(); i < this.arrayOffset() + this.data.limit(); i++)
            {
                byte inB = in.get(pos);
                byte thisB = this.get(i);
                result = Byte.compare(thisB, inB);
                if(result!=0) return result;
            }
            return 0;
        }
    }
}
