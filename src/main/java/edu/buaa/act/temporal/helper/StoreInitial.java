package edu.buaa.act.temporal.helper;

import com.google.common.base.Preconditions;
import edu.buaa.act.temporal.impl.DBMetaInfo;
import edu.buaa.act.temporal.impl.table.MemTableUnsorted;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Created by song on 17-12-6.
 */
public class StoreInitial
{

    private DBMetaInfo metaInfo;
    private File rootDir;


    public StoreInitial(File rootDir)
    {
        this.rootDir = rootDir;

    }

    private DBMetaInfo findAndLoadMeta(File rootDir)
    {
        if(rootDir!=null && rootDir.isDirectory())
        {
            File[] files = rootDir.listFiles();
            if(files!=null)
            {
                for (File f :files)
                {
                    if(f.getName().equals("meta.info")){
                        return new DBMetaInfo(f);
                    }
                }
            }
        }
        return null;
    }

    public DBMetaInfo getMetaInfo() throws Throwable
    {
        DBMetaInfo meta = findAndLoadMeta(rootDir);
        if(meta==null){
            this.metaInfo = new DBMetaInfo(new File(rootDir, "meta.info"));
        }else{
            meta.loadFromDisk();
            this.metaInfo = meta;
        }
        return this.metaInfo;
    }

    public void validate() throws Throwable
    {

    }

    public MemTableUnsorted loadMemtable() throws IOException
    {
        File dumpFile = new File(this.rootDir, "mem.dump");
        if(dumpFile.exists() && dumpFile.isFile())
        {
            MemTableUnsorted memTable = new MemTableUnsorted();
            memTable.load(dumpFile);
        }
        return null;
    }
}
