package edu.buaa.act.temporal.io;

import org.act.temporalProperty.util.ByteBufferSupport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by song on 2018-01-02.
 */
public abstract class TemporalDataFile<READER, WRITER>
{
    /**
     * TABLE_MAGIC_NUMBER was picked by running
     * echo https://github.com/TGraphDB/ | sha1sum
     * and taking the leading 64 bits.
     */
    public static final long FILE_MAGIC_NUMBER = 0xc003bf3c9563aa28L;
    public static final int  FILE_VERSION = 1;

    protected final MappedByteBuffer map;
    protected final DataFileMetaInfo meta;
    protected final FileChannel channel;

    public TemporalDataFile(DataFileMetaInfo metaInfo, FileChannel fileChannel) throws IOException
    {
        this.meta = metaInfo;
        this.channel = fileChannel;
        this.map = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
    }

    public abstract WRITER getWriter() throws IOException;
    public abstract READER getReader();

    public void close() throws IOException
    {
        this.map.force();
        ByteBufferSupport.unmap(this.map);
        this.channel.close();
    }
}
