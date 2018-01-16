package edu.buaa.act.temporal.impl.memlog;

import edu.buaa.act.temporal.io.DataFileMetaInfo;
import edu.buaa.act.temporal.io.TemporalDataFile;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by song on 2018-01-02.
 */
public class MemLogFile extends TemporalDataFile<MemLogFileIteratorReader, MemLogWriter>
{

    public MemLogFile(DataFileMetaInfo metaInfo, FileChannel fileChannel) throws IOException
    {
        super(metaInfo, fileChannel);
    }

    @Override
    public MemLogWriter getWriter() throws IOException
    {
        return new MemLogWriter(this.map);
    }

    @Override
    public MemLogFileIteratorReader getReader()
    {
        return new MemLogFileIteratorReader(this.map);
    }
}
