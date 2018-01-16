package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.impl.table.MemTableSorted;
import edu.buaa.act.temporal.impl.table.PETKey;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Created by song on 17-12-19.
 *
 * method without synchronized because they can be only called from one thread at a time.
 *
 */
public class ManageProcess
{
    private final ReadWriteLock lock;
    private final File rootDir;
    private final DBMetaInfo dbMeta;
    private final TPStorageImpl tpStore;


    private volatile MemTableSorted memTable;
    private volatile boolean shouldRun;
    private Condition writeDiskComplete;

    private PriorityQueue<MergeRequest> mergeRequests = new PriorityQueue<MergeRequest>();

    public ManageProcess(TPStorageImpl tpStorage, DBMetaInfo dbMetaInfo, File rootDir, ReadWriteLock lock, Condition writeDiskComplete)
    {
        this.tpStore = tpStorage;
        this.dbMeta = dbMetaInfo;
        this.rootDir = rootDir;
        this.lock = lock;
        this.writeDiskComplete = writeDiskComplete;

        new Thread(() -> {
            Thread.currentThread().setName("TGraph.TemporalStorage.WRITE");
            runWriteThread();
        }).start();

        new Thread(()-> {
            Thread.currentThread().setName("TGraph.TemporalStorage.MERGE");
            runMergeThread();
        }).start();
    }

    private void runMergeThread()
    {
        try
        {
            while(shouldRun)
            {
                List<Integer> propertyToCheck = new ArrayList<>();
                for(Entry<Integer, Long> entry : memLogSize.entrySet())
                {
                    propertyToCheck.add(entry.getKey());
                }

                for(Integer propertyId : propertyToCheck)
                {
                    mergeIfNeeded(propertyId);
                }
                updateMetaInfo();
            }
        } catch (Throwable e)
        {
            e.printStackTrace();
        }
    }


    private void runWriteThread()
    {
        try
        {
            while(shouldRun)
            {
                if(memTable!=null)
                {
                    writeToDisk(memTable);
                    lock.writeLock().lock();
                    try{
                        memTable = null;
                        writeDiskComplete.signalAll();
                    }finally{
                        lock.writeLock().unlock();
                    }
                }
                else
                {
                    Thread.sleep(10);
                }
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    public void shutdown()
    {
        this.shouldRun = true;
    }

    private void writeToDisk(MemTableSorted memTable) throws IOException
    {
        Map<Integer, NavigableMap<PETKey, ValueAtTime>> data = memTable.allPropertyData();

        for(Entry<Integer, NavigableMap<PETKey, ValueAtTime>> entry : data.entrySet()){
            int propertyId = entry.getKey();
            long size = writeToDisk(propertyId, entry.getValue());
            memLogSize.merge(propertyId, size, (a, b) -> a + b);
        }
    }

    private long writeToDisk(Integer propertyId, NavigableMap<PETKey, ValueAtTime> data) throws IOException
    {
        TemporalPropertyController tpDisk = dbMeta.getTPDisk(propertyId);
        return tpDisk.writeMemLog(data);
    }

    // this is called from a writer thread.
    // the caller should get write lock first.
    public void add(MemTableSorted memTable) throws InterruptedException
    {
        while(this.memTable!=null)
        {
            this.writeDiskComplete.await();
        }
        this.memTable = memTable;
    }


    private boolean needMerge(int propertyId)
    {
        return this.tpStore.getTP(propertyId).needMerge();
    }

    private void mergeIfNeeded(int propertyId)
    {
        this.tpStore.getTP(propertyId).mergeMemLogs();
    }

    private void updateMetaInfo()
    {
        lock.writeLock().lock();
        try{
            writeDiskComplete.signalAll();
        }finally{
            lock.writeLock().unlock();
        }
    }


    private class MergeRequest
    {
        MergeRequest(int propertyId){

        }
    }
}
