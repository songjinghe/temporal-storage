package edu.buaa.act.temporal.impl;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.helper.MemDiskTPMergeIterator;
import edu.buaa.act.temporal.helper.PointToIntervalIterator;
import edu.buaa.act.temporal.helper.SameValueTPMergeIterator;
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
    private final ReentrantReadWriteLock sysLock = new ReentrantReadWriteLock(true);
    private final File rootDir;
    private final Condition waitWrite;
    private final Condition diskFlushComplete;

    private Map<Integer, TemporalPropertyDisk> temporalPropertyMap;

    private MemTableSorted memtable;
    private DBMetaInfo meta;
    private final ManageProcess merger;
    private boolean forbiddenWrite;

    public TPStorageImpl(File dir)
    {
        this.rootDir = dir;
        this.waitWrite = sysLock.writeLock().newCondition();
        this.diskFlushComplete = sysLock.writeLock().newCondition();
        this.merger = new ManageProcess(meta, dir, sysLock, diskFlushComplete);
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
            this.memtable = starter.loadMemtable();
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

//    private class MemDiskMergeIterator implements PeekingIterator<TimeValueEntry>
//    {
//        private final PeekingIterator<TimeValueEntry> diskIter;
//        private final PeekingIterator<TimeValueEntry> memIter;
//
//        TimeValueEntry peeked;
//        private TimePoint validDiskStart;
//
//        public MemDiskMergeIterator(PeekingIterator<TimeValueEntry> memIter, PeekingIterator<TimeValueEntry> diskIter)
//        {
//            this.memIter = memIter;
//            this.diskIter = diskIter;
//            this.peeked = findNextValid();
//        }
//
//        @Override
//        public TimeValueEntry peek()
//        {
//            if(this.peeked!=null) return peeked;
//            else throw new NoSuchElementException();
//        }
//
//        @Override
//        public boolean hasNext()
//        {
//            return this.peeked!=null;
//        }
//
//        @Override
//        public TimeValueEntry next()
//        {
//            TimeValueEntry tmp = this.peeked;
//            this.peeked = findNextValid();
//            return tmp;
//        }
//
//        private TimeValueEntry findNextValid()
//        {
//            while(true)
//            {
//                TimeValueEntry t = findNext();
//                if(t!=null)
//                {
//                    if(!t.getValue().equals(ValueAtTime.Invalid))
//                    {
//                        return t;
//                    }
//                }
//                else
//                {
//                    return null;
//                }
//            }
//        }
//
//        private TimeValueEntry findNext()
//        {
//            TimeValueEntry mem = null, disk = null;
//            while(memIter.hasNext() || diskIter.hasNext())
//            {
//                if(memIter.hasNext()) mem = memIter.peek();
//                if(diskIter.hasNext()) disk = diskIter.peek();
//                if(mem!=null && disk!=null)
//                {
//                    if(mem.getTime().overlap(disk.getTime()))
//                    {
//                        if(mem.getTime().getStart().compareTo(disk.getTime().getStart())<=0)
//                        {
//                            if(mem.getTime().getEnd().hasPost())
//                            {
//                                this.validDiskStart = mem.getTime().getEnd().post();
//                            }else if(mem.getTime().getEnd().isNow()){
//                                this.validDiskStart = TimePoint.NOW;
//                            }else{
//                                this.validDiskStart = TimePoint.NOW;
//                            }
//                            return memIter.next();
//                        }else{
//
//                        }
//                    }else{
//                        if(mem.getTime().compareTo(disk.getTime())<0) //mem comes first
//                        {
//                            return memIter.next();
//                        }else{ // disk first.
//                            return diskIter.next();
//                        }
//                    }
//                }else{
//
//                }
//            }
//        }
//
//        @Override
//        public void remove()
//        {
//            throw new TPSRuntimeException("operation not supported");
//        }
//    }
//
//    private class MemDiskTPMergeIterator implements PeekingIterator<TimeValueEntry>
//    {
//        private final PeekingIterator<TimePointValueEntry> diskIter;
//        private final PeekingIterator<TimePointValueEntry> memIter;
//
//        TimeValueEntry peeked;
//        private TimePoint validDiskStart;
//
//        public MemDiskTPMergeIterator(PeekingIterator<TimePointValueEntry> memIter, PeekingIterator<TimePointValueEntry> diskIter)
//        {
//            this.memIter = memIter;
//            this.diskIter = diskIter;
//            this.peeked = findNextValid();
//        }
//
//        @Override
//        public TimeValueEntry peek()
//        {
//            if(this.peeked!=null) return peeked;
//            else throw new NoSuchElementException();
//        }
//
//        @Override
//        public boolean hasNext()
//        {
//            return this.peeked!=null;
//        }
//
//        @Override
//        public TimeValueEntry next()
//        {
//            TimeValueEntry tmp = this.peeked;
//            this.peeked = findNextValid();
//            return tmp;
//        }
//
//        private TimeValueEntry findNextValid()
//        {
//            while(true)
//            {
//                TimeValueEntry t = findNext();
//                if(t!=null)
//                {
//                    if(!t.getValue().equals(ValueAtTime.Invalid))
//                    {
//                        return t;
//                    }
//                }
//                else
//                {
//                    return null;
//                }
//            }
//        }
//
//        private TimeValueEntry findNext()
//        {
//            TimePointValueEntry mem, disk;
//            while(memIter.hasNext() && diskIter.hasNext())
//            {
//                mem = memIter.peek();
//                disk = diskIter.peek();
//
//                if(mem.getValue().isUnknown())
//                {
//                    memIter.next(); // continue next loop;
//                    continue;
//                }
//
//                if(mem.getTime().compareTo(disk.getTime())<=0)
//                {
//                    memIter.next();
//                    if(memIter.hasNext())
//                    {
//                        TimePointValueEntry end = memIter.peek();
//                        if (end.getTime().hasPre())
//                        {
//                            return new TimeValueEntry(new TimeInterval(mem.getTime(), end.getTime().pre()), mem.getValue());
//                        } else
//                        {
//                            throw new TPSRuntimeException("SNH: time in mem not has pre");
//                        }
//                    }else
//                    {
//                        return new TimeValueEntry(new TimeInterval(mem.getTime(), TimePoint.NOW), mem.getValue());
//                    }
//                }else{
//                    diskIter.next();
//                    if(diskIter.hasNext())
//                    {
//                        TimePointValueEntry end = diskIter.peek();
//                        if(end.getTime().compareTo(mem.getTime())<0)
//                        {
//                            if(end.getTime().hasPre())
//                            {
//                                return new TimeValueEntry(new TimeInterval(disk.getTime(), end.getTime().pre()), disk.getValue());
//                            }else{
//                                throw new TPSRuntimeException("SNH: time in disk not has pre");
//                            }
//                        }
//                        else
//                        {
//                            if(mem.getTime().hasPre())
//                            {
//                                return new TimeValueEntry(new TimeInterval(disk.getTime(), mem.getTime().pre()), disk.getValue());
//                            }else{
//                                throw new TPSRuntimeException("SNH: time in mem not has pre");
//                            }
//                        }
//                    }else
//                    {
//                        if(mem.getTime().hasPre())
//                        {
//                            return new TimeValueEntry(new TimeInterval(disk.getTime(), mem.getTime().pre()), disk.getValue());
//                        }else{
//                            throw new TPSRuntimeException("SNH: time in mem not has pre");
//                        }
//                    }
//                }
//            }
//
//            if(memIter.hasNext())
//            {
//
//            }
//        }
//
//        private TimePointValueEntry getNext()
//        {
//            if(tpIter.hasNext()){
//                this.tpPeeked = tpIter.next();
//                return this.tpPeeked;
//            }else{
//                this.tpPeeked = null;
//                return null;
//            }
//        }
//
//        private TimePointValueEntry getFirstKnown()
//        {
//            if(tpPeeked!=null)
//            {
//                return tpPeeked;
//            }
//            else
//            {
//                while (tpIter.hasNext())
//                {
//                    TimePointValueEntry tmp = tpIter.next();
//                    if (!tmp.getValue().equals(ValueAtTime.Unknown))
//                    {
//                        this.tpPeeked = tmp;
//                        return tmp;
//                    }
//                }
//                return null;
//            }
//        }
//
//        @Override
//        public void remove()
//        {
//            throw new TPSRuntimeException("operation not supported");
//        }
//    }

}
