package edu.buaa.act.temporal.helper;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 17-11-29.
 */
public class TemporalPropertyStoreAdapter implements TemporalPropertyStore {

    @Override
    public Slice getPointValue(long id, int proId, int time) {
        return null;
    }

    @Override
    public Object getRangeValue(long id, int proId, int startTime, int endTime, RangeQueryCallBack callback) {
        return null;
    }

    @Override
    public boolean setProperty(Slice id, byte[] value) {
        return false;
    }

    @Override
    public boolean delete(Slice id) {
        return false;
    }

    @Override
    public void flushMemTable2Disk() {
        //
    }

    @Override
    public void flushMetaInfo2Disk() {
        //
    }

    @Override
    public void shutDown() {
        //
    }
}
