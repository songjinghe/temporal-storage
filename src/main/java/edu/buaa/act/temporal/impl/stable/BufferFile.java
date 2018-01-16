package edu.buaa.act.temporal.impl.stable;

import edu.buaa.act.temporal.impl.memlog.MemLogFile;
import edu.buaa.act.temporal.io.DataFileMetaInfo;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by song on 2018-01-05.
 *
 * Yes, Buffer File is actually and exactly a MemLogFile!
 * except that data in it must within the time range of its corresponding stable file.
 * and a buffer file is usually larger than a memlogfile.
 */
public class BufferFile extends MemLogFile
{
    public BufferFile(DataFileMetaInfo metaInfo, FileChannel fileChannel) throws IOException
    {
        super(metaInfo, fileChannel);
    }
}
