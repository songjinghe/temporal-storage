package edu.buaa.act.temporal.io;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.memlog.MemLogFile;
import edu.buaa.act.temporal.impl.memlog.MemLogReader;
import edu.buaa.act.temporal.impl.memlog.MemLogWriter;
import edu.buaa.act.temporal.impl.stable.BufferFile;
import edu.buaa.act.temporal.impl.stable.StableFile;
import edu.buaa.act.temporal.impl.unstable.UnStableFile;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by song on 17-12-30.
 */
public class DiskFilesManager
{
    private final File rootDir;
    private LoadingCache<DataFileMetaInfo, DataFileCacheItem> cache;

    public DiskFilesManager(final File dbDir, long currentFileId){
        CacheLoader<DataFileMetaInfo, DataFileCacheItem> loader = new CacheLoader<DataFileMetaInfo, DataFileCacheItem>()
        {
            public DataFileCacheItem load(DataFileMetaInfo key) throws IOException
            {
                return new DataFileCacheItem(dbDir, key);
            }
        };

        this.cache = CacheBuilder.newBuilder().maximumSize(200).build(loader);
        this.rootDir = dbDir;
    }

    public TemporalDataFile get(DataFileMetaInfo metaInfo)
    {
        try
        {
            return cache.get(metaInfo).temporalDataFile();
        } catch (ExecutionException e)
        {
            e.printStackTrace();
            throw new TPSRuntimeException(e);
        }
    }

    public void invalid(DataFileMetaInfo meta)
    {
        cache.invalidate(meta);
    }

    public void close()
    {
        cache.invalidateAll();
    }

    public MemLogWriter latestMemLogWriter(int propertyId) throws IOException
    {
        File proDir = new File(rootDir, String.valueOf(propertyId));
        if(proDir.exists())
        {
            if(proDir.isDirectory() && proDir.canWrite() && proDir.canExecute())
            {
                Entry<Integer, File> maxIdFile = FileNames.maxIdFile(FileNames.memLogFiles(proDir.listFiles()));
                File fileToWrite;
                if(maxIdFile!=null)
                {
                    if(maxIdFile.getValue().length()>4*1024*1024){
                        fileToWrite = new File(proDir, String.valueOf(maxIdFile.getKey()+1));
                    }else{
                        fileToWrite = new File(proDir, String.valueOf(maxIdFile.getKey()));
                    }
                }else{
                    fileToWrite = new File(proDir, String.valueOf(0));
                    if(!fileToWrite.createNewFile()){
                        throw new IOException("unable to create file!");
                    }
                }
                return new MemLogWriter(fileToWrite);
            }else{
                throw new IOException("unable to write file!");
            }
        }else{
            if(!proDir.mkdir()){
                throw new IOException("unable to create property dir!");
            }
            File fileToWrite = new File(proDir, String.valueOf(0));
            if(!fileToWrite.createNewFile()){
                throw new IOException("unable to create file!");
            }
            return new MemLogWriter(fileToWrite);
        }
    }

    public long nextFileId()
    {
        return this.id.getAndIncrement();
    }

    public MemLogFile getMemLog(DataFileMetaInfo metaInfo)
    {
        TemporalDataFile obj = get(metaInfo);
        if(obj instanceof MemLogFile){
            return (MemLogFile) obj;
        }else{
            throw new TPSRuntimeException("SNH: not a memlog file!");
        }
    }

    public UnStableFile getUnStable(DataFileMetaInfo meta)
    {
        TemporalDataFile obj = get(meta);
        if(obj instanceof UnStableFile){
            return (UnStableFile) obj;
        }else{
            throw new TPSRuntimeException("SNH: not a unstable file!");
        }
    }

    public StableFile getStable(DataFileMetaInfo meta)
    {
        TemporalDataFile obj = get(meta);
        if(obj instanceof StableFile){
            return (StableFile) obj;
        }else{
            throw new TPSRuntimeException("SNH: not a unstable file!");
        }
    }

    public BufferFile getBuffer(DataFileMetaInfo stableMeta)
    {
        DataFileMetaInfo bufferMeta = new DataFileMetaInfo(stableMeta.getId(), stableMeta.getPropertyId(), DataFileMetaInfo.TYPE.STABLE_BUFFER);
        TemporalDataFile obj = get(bufferMeta);
        if(obj instanceof BufferFile){
            return (BufferFile) obj;
        }else{
            throw new TPSRuntimeException("SNH: not a unstable file!");
        }
    }

    public void delete(DataFileMetaInfo fileMetaInfo)
    {
        this.invalid(fileMetaInfo);
        File dataFile = FileNames.getPath(dbDir, metaInfo);
    }

    private static class DataFileCacheItem
    {
        private final FileChannel fileChannel;
        private final TemporalDataFile tdf;

        public DataFileCacheItem(File dbDir, DataFileMetaInfo metaInfo) throws IOException
        {
            File dataFile = FileNames.getPath(dbDir, metaInfo);
            fileChannel = new RandomAccessFile(dataFile,"rw").getChannel();
            try {
                switch (metaInfo.getType()){
                    case MEM_LOG: tdf = new MemLogFile(metaInfo, fileChannel);
                        break;
                    case UN_STABLE: tdf = new UnStableFile(metaInfo, fileChannel);
                        break;
                    case STABLE: tdf = new StableFile(metaInfo, fileChannel);
                        break;
                    case STABLE_BUFFER: tdf = new BufferFile(metaInfo, fileChannel);
                        break;
                    case INDEX_STABLE: tdf = new TemporalDataFile(metaInfo);
                        break;
                    case INDEX_UN_STABLE: tdf = new TemporalDataFile(metaInfo);
                        break;
                    default: tdf=null;
                }
            }
            catch (IOException e) {
                closeQuietly(fileChannel);
                throw e;
            }
        }

        public TemporalDataFile temporalDataFile()
        {
            return tdf;
        }

        private void closeQuietly(Closeable closeable){
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            }
            catch (IOException ignored) {
            }
        }
    }

}
