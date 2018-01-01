package edu.buaa.act.temporal;



import edu.buaa.act.temporal.exception.TPSException;
import edu.buaa.act.temporal.impl.TPStorageImpl;
import edu.buaa.act.temporal.impl.TemporalValue;

import java.io.File;
import java.util.List;

/**
 * Created by song on 17-12-5.
 */
public interface TemporalPropertyStorage
{
    String version = "TGraph-TemporalStorage V2.3.12-201712";

    // READ

    ValueAtTime getValueAtTimePoint(int propertyId, long entityId, TimePoint time);

//    TemporalValue getValuesDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end);

    void getValuesCallBack(int propertyId, long entityId, TimePoint timePoint, TimeValueEntryCallback callback);

    // WRITE

    void setValueDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end, ValueAtTime value );

    void deleteValueDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end);

    void deleteTemporalProperty(int propertyId);


    // INDEX

    List<TemporalIndex> listIndexes();

    TemporalIndex getIndexById(int indexId);

    boolean createOuterIndex(int propertyId, TimePoint start, TimePoint end);

    boolean createAggregationIndex(int propertyId, TimePoint start, TimePoint end, AggregationCallback callback);

    boolean deleteIndex(int indexId);

    List<Integer> getEntityIdByTimeAndValue(int propertyId, TimeInterval timeRange, ValueAtTime valueMin, ValueAtTime valueMax);


    // LOG & RECOVERY

    void flushAll();


    // SYSTEM

    void start() throws Throwable;

    void stop() throws Throwable;


    static TemporalPropertyStorage newInstance(File dbDir) throws TPSException
    {
        if(dbDir.exists() && dbDir.isDirectory())
        {
            return new TPStorageImpl(dbDir);
        }else{
            throw new TPSException(dbDir.getAbsolutePath()+" not exist or is not a directory.");
        }
    }

}
