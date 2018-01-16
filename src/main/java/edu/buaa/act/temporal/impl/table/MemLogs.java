package edu.buaa.act.temporal.impl.table;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.io.FileNames;
import edu.buaa.act.temporal.impl.memlog.MemLogReader;
import edu.buaa.act.temporal.impl.memlog.MemLogWriter;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by song on 2018-01-01.
 */
public class MemLogs
{
    private final int propertyId;
    private final File dir;
    private Map<Integer, MemLogReader> readers;
    private Map<Integer, MemLogWriter> writers;

    public MemLogs(File dir, int propertyId)
    {
        this.dir = dir;
        this.propertyId = propertyId;
    }

    public MemLogWriter latestMemLogWriter() throws IOException
    {

    }

    private File fileToWrite() throws IOException
    {
        if(dir.exists())
        {
            if(dir.isDirectory() && dir.canWrite() && dir.canExecute())
            {
                Map.Entry<Integer, File> maxIdFile = FileNames.maxIdFile(FileNames.memLogFiles(dir.listFiles()));
                File fileToWrite;
                if(maxIdFile!=null)
                {
                    if(maxIdFile.getValue().length()>4*1024*1024){
                        fileToWrite = new File(dir, String.valueOf(maxIdFile.getKey()+1));
                    }else{
                        fileToWrite = new File(dir, String.valueOf(maxIdFile.getKey()));
                    }
                }else{
                    fileToWrite = new File(dir, String.valueOf(0));
                    if(!fileToWrite.createNewFile()){
                        throw new IOException("unable to create file!");
                    }
                }
                return fileToWrite;
            }else{
                throw new IOException("unable to write file!");
            }
        }else{
            if(!dir.mkdir()){
                throw new IOException("unable to create property dir!");
            }
            File fileToWrite = new File(dir, String.valueOf(0));
            if(!fileToWrite.createNewFile()){
                throw new IOException("unable to create file!");
            }
            return fileToWrite;
        }
    }

    public PeekingIterator<ETVEntry> getMergeReadIterator()
    {

        return null;
    }
}
