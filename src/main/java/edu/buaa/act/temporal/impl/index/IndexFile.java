package edu.buaa.act.temporal.impl.index;

import edu.buaa.act.temporal.io.DataFileMetaInfo;
import edu.buaa.act.temporal.io.TemporalDataFile;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by song on 2018-01-09.
 */
public class IndexFile extends TemporalDataFile<IndexFileReader, IndexFileWriter>
{

    public IndexFile(DataFileMetaInfo metaInfo, FileChannel fileChannel) throws IOException
    {
        super(metaInfo, fileChannel);
    }

    @Override
    public IndexFileWriter getWriter() throws IOException
    {
        return new IndexFileWriter(this.map);


        //return null;
    }

    @Override
    public IndexFileReader getReader()
    {
        //return null;
    }
}
