package edu.buaa.act.temporal.impl.unstable;

import edu.buaa.act.temporal.io.DataFileMetaInfo;
import edu.buaa.act.temporal.io.TemporalDataFile;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by song on 2018-01-02.
 */
public class UnStableFile extends TemporalDataFile<UnStableFileReader, UnStableFileWriter>
{

    public UnStableFile(DataFileMetaInfo metaInfo, FileChannel fileChannel) throws IOException
    {
        super(metaInfo, fileChannel);
    }

    @Override
    public UnStableFileWriter getWriter() throws IOException
    {
        return new UnStableFileWriter(this.map);
    }

    @Override
    public UnStableFileReader getReader()
    {
        return new UnStableFileReader(this.map);
    }
}
