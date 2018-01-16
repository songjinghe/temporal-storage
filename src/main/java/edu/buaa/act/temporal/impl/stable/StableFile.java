package edu.buaa.act.temporal.impl.stable;

import edu.buaa.act.temporal.io.DataFileMetaInfo;
import edu.buaa.act.temporal.io.TemporalDataFile;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by song on 2018-01-03.
 */
public class StableFile extends TemporalDataFile<StableFileReader, StableFileWriter>
{


    public StableFile(DataFileMetaInfo metaInfo, FileChannel fileChannel) throws IOException
    {
        super(metaInfo, fileChannel);
    }

    @Override
    public StableFileWriter getWriter() throws IOException
    {
        return new StableFileWriter(this.map);
    }

    @Override
    public StableFileReader getReader()
    {
        return new StableFileReader(this.map);
    }
}
