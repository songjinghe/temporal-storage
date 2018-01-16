package edu.buaa.act.temporal.impl;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.memlog.*;
import edu.buaa.act.temporal.impl.stable.*;
import edu.buaa.act.temporal.impl.table.*;
import edu.buaa.act.temporal.impl.unstable.ETVEntryAppendIterator;
import edu.buaa.act.temporal.impl.unstable.UnStableFile;
import edu.buaa.act.temporal.impl.unstable.UnStableFileReader;
import edu.buaa.act.temporal.impl.unstable.UnStableFileWriter;
import edu.buaa.act.temporal.io.DataFileMetaInfo;
import edu.buaa.act.temporal.io.DiskFilesManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 17-12-6.
 */
public class TemporalPropertyController
{
    private final DiskFilesManager filesManager;
    // lock for current property
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // above lock is used to protect the meta below
    private final TPMetaInfo meta;

    public TemporalPropertyController(TPMetaInfo meta, DiskFilesManager files)
    {
        this.meta = meta;
        this.filesManager = files;
    }

    public ValueAtTime getValue(long entityId, TimePoint time)
    {
        lock.readLock().lock();
        try{

        }finally{
            lock.readLock().unlock();
        }
    }


    public int getPropertyId()
    {
        return meta.getPropertyId();
    }

    public PeekingIterator<TimePointValueEntry> getTimePointValues(long entityId, TimePoint timePoint)
    {
        lock.readLock().lock();
        try{

        }finally{
            lock.readLock().unlock();
        }
        return null;
    }

    public void delete()
    {
        lock.writeLock().lock();
        try{

        }finally{
            lock.writeLock().unlock();
        }
    }

    public long writeMemLog(NavigableMap<PETKey, ValueAtTime> data) throws IOException
    {
        long size = 0;
        MemLogWriter writer = filesManager.getMemLog(rotationControl(meta.latestMemLog())).getWriter();
        writer.beginBlock();
        for(Map.Entry<PETKey, ValueAtTime> entry : data.entrySet())
        {
            PETKey key = entry.getKey();
            writer.append(key.getEntityId(), key.getTime(), entry.getValue());
            size+=(8+TimePoint.IO.rawSize()+entry.getValue().length());
        }
        writer.endBlock();
        return size;
    }

    private DataFileMetaInfo rotationControl(DataFileMetaInfo latestMemLog)
    {
        if(latestMemLog==null || latestMemLog.getFileSize()>4*1024*1024)
        {
            long id = System.currentTimeMillis();
            return new DataFileMetaInfo(id, this.getPropertyId(), DataFileMetaInfo.TYPE.MEM_LOG);
        }else{
            return latestMemLog;
        }
    }

    public void mergeMemLogs() throws IOException
    {
        List<DataFileMetaInfo> memLogFiles;
        List<DataFileMetaInfo> unStableFiles;
        DataFileMetaInfo stForCheckPoint;
        TimePoint stMaxTime;

        lock.readLock().lock(); // only get a consistent view of files, do not care about further append.
        try{
            if(!meta.needMergeMemLogFiles()) return;

            memLogFiles = meta.getMemLogToMerge();
            unStableFiles = meta.getUnStableToMerge();
            stForCheckPoint = meta.getLatestStable();
            stMaxTime = meta.stableMaxTime();
        }finally{
            lock.readLock().unlock(); // so we unlock it right after reading to allowed append operations.
        }

        PeekingIterator<ETVEntry> m = newMemLogIterator(memLogFiles);
        if(m==null) throw new TPSRuntimeException("SNH: no need to merge");
        m = new AfterTimePointFilterIterator(m, stMaxTime);
        ETVEntryAppendIterator u = newUnstableIterator(unStableFiles);

        DataFileMetaInfo target;

        if(u.isEmpty()) // no unstable files, will merge to a level 0 unstable file.
        {
            target = new DataFileMetaInfo(0, this.getPropertyId(), DataFileMetaInfo.TYPE.UN_STABLE);
            UnStableFile targetFile = filesManager.getUnStable(target);
            UnStableFileWriter writer = targetFile.getWriter();
            writer.consumeAndBuild(m);
        }
        else if(u.size()==6) // unstable file is full, will merge to a stable file.
        {
            long fileId = meta.getNextStableId();
            target = new DataFileMetaInfo(fileId, this.getPropertyId(), DataFileMetaInfo.TYPE.STABLE);

            PeekingIterator<ETVEntry> iterReader;
            if(stForCheckPoint!=null)
            {
                iterReader = new ETVEntryMergeIterator(u, m);
                StableFile stableToQuery = filesManager.getStable(stForCheckPoint);
                StableFileReader stReader = stableToQuery.getReader();
                iterReader = new MergeWithCheckPointIterator( stReader, iterReader );

                if(stForCheckPoint.hasBufferFile()){
                    BufferFile bufferFile = filesManager.getBuffer(stForCheckPoint);
                    MemLogFileIteratorReader reader = bufferFile.getReader();
                    iterReader = new ETVEntryMergeIterator(iterReader, reader);
                }
            }else{
                iterReader = new ETVEntryMergeIterator(u, m);
            }
            StableFile targetFile = filesManager.getStable(target);
            StableFileWriter writer = targetFile.getWriter();
            writer.consumeAndBuild(iterReader);

            PeekingIterator<ETVEntry> memLogIterator = new BeforeTimePointFilterIterator(newMemLogIterator(memLogFiles),stMaxTime);

        }
        else // unstable not full, merge to a unstable file.
        {
            long fileId = unStableFiles.size();
            target = new DataFileMetaInfo(fileId, this.getPropertyId(), DataFileMetaInfo.TYPE.UN_STABLE);
            UnStableFile targetFile = filesManager.getUnStable(target);
            UnStableFileWriter writer = targetFile.getWriter();
            writer.consumeAndBuild(new ETVEntryMergeIterator(u, m));
        }

        lock.writeLock().lock();
        try{
            if(target.getType()== DataFileMetaInfo.TYPE.STABLE){
                meta.addStable(target);
            }else{
                meta.addUnStable(target);
            }
            for(DataFileMetaInfo fileMetaInfo : memLogFiles){
                meta.removeMemLog(fileMetaInfo);
                filesManager.delete(fileMetaInfo);
            }
            for(DataFileMetaInfo fileMetaInfo : memLogFiles){
                meta.removeUnStable(fileMetaInfo);
                filesManager.delete(fileMetaInfo);
            }
        }finally{
            lock.writeLock().unlock();
        }
    }

    private ETVEntryAppendIterator newUnstableIterator(List<DataFileMetaInfo> unStableFiles)
    {
        ETVEntryAppendIterator u = new ETVEntryAppendIterator();
        for(int i=0;i<unStableFiles.size(); i++)
        {
            DataFileMetaInfo usMeta = unStableFiles.get(i);
            UnStableFile file = filesManager.getUnStable(usMeta);
            UnStableFileReader iter = file.getReader();
            u.addFirst(iter);
        }
        return u;
    }

    private PeekingIterator<ETVEntry> newMemLogIterator(List<DataFileMetaInfo> memLogFiles)
    {
        PeekingIterator<ETVEntry> m = null;
        for(int i=0; i<memLogFiles.size(); i++)
        {
            DataFileMetaInfo memFile = memLogFiles.get(i);
            MemLogFile file = filesManager.getMemLog(memFile);
            MemLogFileIteratorReader iter = file.getReader();
            if(i==0) m = iter;
            else m = new ETVEntryMergeIterator(m, iter);
        }
        return m;
    }

    public void mergeBufferFile(int stableFileId)
    {

    }


}
