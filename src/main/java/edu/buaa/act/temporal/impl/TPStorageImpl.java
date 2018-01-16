package edu.buaa.act.temporal.impl;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.iterator.MemDiskTPMergeIterator;
import edu.buaa.act.temporal.impl.iterator.PointToIntervalIterator;
import edu.buaa.act.temporal.impl.iterator.SameValueTPMergeIterator;
import edu.buaa.act.temporal.impl.table.MemTableSorted;
import edu.buaa.act.temporal.helper.StoreInitial;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 17-12-6.
 */
public class TPStorageImpl implements TemporalPropertyStorage
{
    private final ReentrantReadWriteLock sysLock = new ReentrantReadWriteLock();
    private final File rootDir;
    private final Condition waitWrite;
    private final Condition diskFlushComplete;

    private Map<Integer, TemporalPropertyController> temporalPropertyMap;

    private MemTableSorted memtable;
    private DBMetaInfo meta;
    private final ManageProcess merger;
    private boolean forbiddenWrite;

    public TPStorageImpl(File dir)
    {
        this.rootDir = dir;
        this.waitWrite = sysLock.writeLock().newCondition();
        this.diskFlushComplete = sysLock.writeLock().newCondition();
        this.merger = new ManageProcess(this, meta, dir, sysLock, diskFlushComplete);
    }

    @Override
    public ValueAtTime getValueAtTimePoint(int propertyId, long entityId, TimePoint time)
    {
        sysLock.readLock().lock();
        try
        {
            ValueAtTime value = this.memtable.getValueAtTimePoint(propertyId, entityId, time);
            if (value != null)
            {
                return value;
            } else
            {
                // not found in memTable.
                return getTP(propertyId).getValue(entityId, time);
            }
        }finally
        {
            sysLock.readLock().unlock();
        }
    }

    @Override
    public void getValuesCallBack(int propertyId, long entityId, TimePoint timePoint, TimeValueEntryCallback callback)
    {
        sysLock.readLock().lock();
        try
        {
            PeekingIterator<TimePointValueEntry> memIter = this.memtable.getTimePointValues(propertyId, entityId, timePoint);
            PeekingIterator<TimePointValueEntry> diskIter = this.getTP(propertyId).getTimePointValues(entityId, timePoint);
            PeekingIterator<TimePointValueEntry> mergedTPIter = new SameValueTPMergeIterator(new MemDiskTPMergeIterator(memIter, diskIter));

            PeekingIterator<TimeValueEntry> merged = new PointToIntervalIterator(mergedTPIter);

            while(merged.hasNext())
            {
                TimeValueEntry tv = merged.next();
                boolean shouldContinue = callback.onTimeInterval(tv.getTime(), tv.getValue());
                if(!shouldContinue) break;
            }
            callback.onFinish();
        }finally
        {
            sysLock.readLock().unlock();
        }
    }

    /**
     * //TODO: Rewrite this method with custom RWLock which support upgrade and downgrade, so that read op are not blocked when merging.
     * perform lock downgrade operation which is provided by ReentrantReadWriteLock;
     * downgrade so that when submit MemTable to manage thread (merging), read operation
     * is not blocked.
     * also when submitting, other threads can not write.
     */
    @Override
    public void setValueDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end, ValueAtTime value)
    {
        Throwable err = null;

        sysLock.writeLock().lock();
        try
        {
            if(forbiddenWrite){
                waitWrite.await();
            }

            this.memtable.set(propertyId, entityId, start, end, value);

            if (this.memtable.getCurrentSize() > 16 * 1024 * 1024) // 16MB
            {
                forbiddenWrite = true;
                this.merger.add(this.memtable);
                this.memtable = new MemTableSorted();
                forbiddenWrite = false;
                waitWrite.signalAll();
            }
        }
        catch(InterruptedException e){err = e;}
        finally
        {
            sysLock.writeLock().unlock(); // lock downgrade
        }

        if(err!=null){
            err.printStackTrace();
            throw new TPSRuntimeException(err);
        }
    }

    @Override
    public void deleteValueDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end)
    {
        sysLock.writeLock().lock();
        try
        {
            this.memtable.set(propertyId, entityId, start, end, ValueAtTime.Invalid);
        }finally
        {
            sysLock.writeLock().unlock();
        }
    }

    @Override
    public void deleteTemporalProperty(int propertyId) {
        sysLock.writeLock().lock();
        try
        {
            this.memtable.deleteTemporalProperty(propertyId);
            this.getTP(propertyId).delete();
        }finally
        {
            sysLock.writeLock().unlock();
        }
    }

    @Override
    public List<TemporalIndex> listIndexes() {
        return null;
    }

    @Override
    public TemporalIndex getIndexById(int indexId) {
        return null;
    }

    @Override
    public boolean createOuterIndex(int propertyId, TimePoint start, TimePoint end) {
        return false;
    }

    @Override
    public boolean createAggregationIndex(int propertyId, TimePoint start, TimePoint end, AggregationCallback callback) {
        return false;
    }

    @Override
    public boolean deleteIndex(int indexId) {
        return false;
    }

    @Override
    public List<Integer> getEntityIdByTimeAndValue(int propertyId, TimeInterval timeRange, ValueAtTime valueMin, ValueAtTime valueMax) {
        return null;
    }

    @Override
    public void flushAll() {
        sysLock.readLock().lock();
        try{

        }finally
        {
            sysLock.readLock().unlock();
        }
    }

    @Override
    public void start() throws Throwable
    {
        sysLock.writeLock().lock();
        try
        {
            StoreInitial starter = new StoreInitial(this.rootDir);
            this.meta = starter.getMetaInfo();
            this.memtable = starter.loadMemTable();
        }finally
        {
            sysLock.writeLock().unlock();
        }
    }

    @Override
    public void stop() throws Throwable
    {
        sysLock.writeLock().lock();
        try{
            flushAll();
        }finally
        {
            sysLock.writeLock().unlock();
        }
    }

    public TemporalPropertyController getTP(int propertyId)
    {
        return this.temporalPropertyMap.get(propertyId);
    }



}
