package edu.buaa.act.temporal.helper;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by song on 17-12-19.
 */
public class FileByteBuffer implements DataInput, DataOutput
{
    private final File file;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final long PAGE_SIZE=4096;

    public FileByteBuffer(File file) throws IOException
    {
        this.file = file;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    public String getString(int len)
    {
        byte[] arr = new byte[len];
        mappedByteBuffer.get(arr);
        return new String(arr);
    }

    public int getInt()
    {
        return this.mappedByteBuffer.getInt();
    }

    public int getByte()
    {
        return this.mappedByteBuffer.get();
    }

    public void close() throws IOException
    {
        this.fileChannel.close();
    }

    public void putStr(String str, int len)
    {
        this.mappedByteBuffer.put(str.getBytes(), this.mappedByteBuffer.position(), len);
    }

    public void putInt(int value)
    {
        this.mappedByteBuffer.putInt(value);
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        //
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        //
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        //return 0;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        //return false;
    }

    @Override
    public byte readByte() throws IOException
    {
        //return 0;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        //return 0;
    }

    @Override
    public short readShort() throws IOException
    {
        //return 0;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        //return 0;
    }

    @Override
    public char readChar() throws IOException
    {
        //return 0;
    }

    @Override
    public int readInt() throws IOException
    {
        //return 0;
    }

    @Override
    public long readLong() throws IOException
    {
        //return 0;
    }

    @Override
    public float readFloat() throws IOException
    {
        //return 0;
    }

    @Override
    public double readDouble() throws IOException
    {
        //return 0;
    }

    @Override
    public String readLine() throws IOException
    {
        //return null;
    }

    @Override
    public String readUTF() throws IOException
    {
        //return null;
    }

    @Override
    public void write(int b) throws IOException
    {
        //
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        //
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        //
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        //
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        //
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        //
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        //
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        //
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        //
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        //
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        //
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        //
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        //
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        //
    }
}
