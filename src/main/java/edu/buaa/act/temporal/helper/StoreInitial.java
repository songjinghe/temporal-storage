package edu.buaa.act.temporal.helper;

import edu.buaa.act.temporal.exception.TPSException;
import edu.buaa.act.temporal.impl.DBMetaInfo;
import edu.buaa.act.temporal.impl.table.MemTableSorted;

import java.io.File;
import java.io.IOException;

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
            this.metaInfo = loadMemTable();
        }
        return this.metaInfo;
    }

    public void validate() throws Throwable
    {

    }

//    public MemTableUnsorted loadMemTable() throws IOException
//    {
//        File dumpFile = new File(this.rootDir, "mem.dump");
//        if(dumpFile.exists() && dumpFile.isFile())
//        {
//            MemTableUnsorted memTable = new MemTableUnsorted();
//            memTable.load(dumpFile);
//        }
//        return null;
//    }

    public MemTableSorted loadMemTable() throws IOException, TPSException
    {
        File dumpFile = new File(this.rootDir, "mem.dump");
        if(dumpFile.exists() && dumpFile.isFile())
        {
            MemTableSorted memTable = new MemTableSorted();
            memTable.load(dumpFile);
            return memTable;
        }else{
            throw new TPSException("load memtable failed.");
        }
    }

    public void loadMetaInfo() throws Throwable
    {

//            byte[] data = Files.readAllBytes(this.file.toPath());
//            ByteArrayDataInput in = ByteStreams.newDataInput(data);
//
//            int contentLen = in.readInt();
//            if (data.length != contentLen + 4 + 32)
//            {
//                throw new RuntimeException("not a valid storage");
//            }
//            HashCode hashResult = Hashing.md5().hashBytes(data, 4, data.length - 32 - 4);
//            HashCode hashCode = HashCode.fromBytes(ByteSource.wrap(data).slice(data.length - 32, 32).read());
//            if (!hashCode.equals(hashResult))
//            {
//                throw new RuntimeException("hash not match!");
//            }
//
//            int strLen = in.readInt();
//            byte[] strContent = new byte[strLen];
//            in.readFully(strContent);
////            TemporalPropertyStorage.version = new String(strContent);
//
//            this.lastUpdateTime = in.readLong();
//
//            int tpMapSize = in.readInt();
//            for (int i = 0; i < tpMapSize; i++)
//            {
//                TemporalPropertyDisk tp = TemporalPropertyDisk.decode(in);
//                tpMap.put(tp.getId(), tp);
//            }
//
//            int indexSize = in.readInt();
//            for (int i = 0; i < indexSize; i++)
//            {
//                TPIndex index = TPIndex.decode(in);
//                indexMap.put(index.getId(), index);
//            }
    }

    public void flushToDisk() throws IOException
    {

//            ByteArrayDataOutput out = ByteStreams.newDataOutput();
//            out.writeInt(TemporalPropertyStorage.version.length());
//            out.writeChars(TemporalPropertyStorage.version);
//            out.writeLong(lastUpdateTime);
//
//            out.writeInt(this.tpMap.size());
//            for (TemporalPropertyDisk tp : tpMap.values())
//            {
//                tp.encode(out);
//            }
//
//            out.writeInt(indexMap.size());
//            for (TPIndex index : indexMap.values())
//            {
//                index.encode(out);
//            }
//
//            byte[] content = out.toByteArray();
//            HashCode hash = Hashing.md5().hashBytes(content);
//
//            ByteArrayOutputStream bs = new ByteArrayOutputStream();
//            FileOutputStream fs = new FileOutputStream(this.file);
//            bs.writeTo(fs);
//            bs.write(Ints.toByteArray(content.length));
//            bs.write(content);
//            bs.write(hash.asBytes());
//            bs.close();
//            fs.close();
    }
}
