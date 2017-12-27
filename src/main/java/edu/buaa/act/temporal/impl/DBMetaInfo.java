package edu.buaa.act.temporal.impl;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;

import edu.buaa.act.temporal.TemporalPropertyStorage;
import edu.buaa.act.temporal.impl.index.TPIndex;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 17-12-6.
 */
public class DBMetaInfo
{

    private final File file;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();


    private long lastUpdateTime = System.currentTimeMillis();

    private Map<Integer, TemporalPropertyDisk> tpMap = new HashMap<>();
    private Map<Integer, TPIndex> indexMap = new HashMap<>();


    public DBMetaInfo(File dbMetaFile)
    {
        this.file = dbMetaFile;
    }

    public void loadFromDisk() throws Throwable
    {
        lock.writeLock().lock();
        try
        {
            byte[] data = Files.readAllBytes(this.file.toPath());
            ByteArrayDataInput in = ByteStreams.newDataInput(data);

            int contentLen = in.readInt();
            if (data.length != contentLen + 4 + 32)
            {
                throw new RuntimeException("not a valid storage");
            }
            HashCode hashResult = Hashing.md5().hashBytes(data, 4, data.length - 32 - 4);
            HashCode hashCode = HashCode.fromBytes(ByteSource.wrap(data).slice(data.length - 32, 32).read());
            if (!hashCode.equals(hashResult))
            {
                throw new RuntimeException("hash not match!");
            }

            int strLen = in.readInt();
            byte[] strContent = new byte[strLen];
            in.readFully(strContent);
//            TemporalPropertyStorage.version = new String(strContent);

            this.lastUpdateTime = in.readLong();

            int tpMapSize = in.readInt();
            for (int i = 0; i < tpMapSize; i++)
            {
                TemporalPropertyDisk tp = TemporalPropertyDisk.decode(in);
                tpMap.put(tp.getId(), tp);
            }

            int indexSize = in.readInt();
            for (int i = 0; i < indexSize; i++)
            {
                TPIndex index = TPIndex.decode(in);
                indexMap.put(index.getId(), index);
            }

        }finally
        {
            lock.writeLock().unlock();
        }
    }

    public void flushToDisk() throws IOException
    {
        lock.writeLock().lock();
        try
        {
            ByteArrayDataOutput out = ByteStreams.newDataOutput();
            out.writeInt(TemporalPropertyStorage.version.length());
            out.writeChars(TemporalPropertyStorage.version);
            out.writeLong(lastUpdateTime);

            out.writeInt(this.tpMap.size());
            for (TemporalPropertyDisk tp : tpMap.values())
            {
                tp.encode(out);
            }

            out.writeInt(indexMap.size());
            for (TPIndex index : indexMap.values())
            {
                index.encode(out);
            }

            byte[] content = out.toByteArray();
            HashCode hash = Hashing.md5().hashBytes(content);

            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            FileOutputStream fs = new FileOutputStream(this.file);
            bs.writeTo(fs);
            bs.write(Ints.toByteArray(content.length));
            bs.write(content);
            bs.write(hash.asBytes());
            bs.close();
            fs.close();
        }finally
        {
            lock.writeLock().unlock();
        }
    }



}
