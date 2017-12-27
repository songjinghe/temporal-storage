package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.helper.DataEntryUtils;
import edu.buaa.act.temporal.impl.table.MemTableSorted;
import edu.buaa.act.temporal.impl.table.MemTableUnsorted;
import edu.buaa.act.temporal.helper.StoreInitial;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 17-12-6.
 */
public class TPStorageImpl implements TemporalPropertyStorage
{
    private ReadWriteLock sysLock = new ReentrantReadWriteLock();
    private File rootDir;

    private Map<Integer, TemporalPropertyDisk> temporalPropertyMap;

    private MemTableSorted memtable;
    private DBMetaInfo meta;
    private ManageProcess manager;

    public TPStorageImpl(File dir)
    {
        this.rootDir = dir;
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

//    @Override
//    public TemporalValue getValuesDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end)
//    {
//        sysLock.readLock().lock();
//        try
//        {
//            TimeInterval timeRange = new TimeInterval(start, end);
//
//            TemporalValue valueInMem = this.memtable.getTemporalValue(propertyId, entityId, timeRange);
//            List<TimeValueEntry> valueInDisk = this.getTP(propertyId).getValues(entityId, timeRange);
//            return DataEntryUtils.mergeList(valueInMem, valueInDisk);
//        }finally
//        {
//            sysLock.readLock().unlock();
//        }
//    }

    @Override
    public void getValuesCallBack(int propertyId, long entityId, TimePoint timePoint, TimeValueEntryCallback callback)
    {
        sysLock.readLock().lock();
        try
        {
            this.memtable.getTimeValues(propertyId, entityId, timePoint, callback);
            getTP(propertyId).getCallback(entityId, timePoint, callback);
        }finally
        {
            sysLock.readLock().unlock();
        }
    }

    @Override
    public void setValueDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end, ValueAtTime value) {
        sysLock.writeLock().lock();
        try
        {
            this.memtable.setValueDuringTimeInterval(propertyId, entityId, start, end, value);
        }finally
        {
            sysLock.writeLock().unlock();
        }
    }

    @Override
    public void deleteValueDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end)
    {
        sysLock.writeLock().lock();
        try
        {
            this.memtable.setValueDuringTimeInterval(propertyId, entityId, start, end, ValueAtTime.Invalid);
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
            this.memtable = starter.loadMemtable();
            this.manager = new ManageProcess();
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

    private TemporalPropertyDisk getTP(int propertyId)
    {
        return this.temporalPropertyMap.get(propertyId);
    }
}
