package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.query.range.InternalEntryRangeQueryCallBack;
import org.act.temporalProperty.util.Slice;

/**
 * Users who want custom aggregation query should extend this class
 * Created by song on 2018-04-01.
 */
public abstract class IntervalAggregationQuery<K,V> implements AggregationQuery<K,V>, InternalEntryRangeQueryCallBack {
    private int time = -1;
    private Slice lastValue;

    @Override
    public void onNewEntry(InternalEntry entry) {
        InternalKey key = entry.getKey();
        int curT = key.getStartTime();
        int t;
        if(time==-1) {
            t = computeStartTime();
        }else{
            t = computeNextTime(time);
        }
        while(t<curT) {
            addNewValue(t, lastValue);
            t = computeNextTime(t);
        }
        addNewValue(t, entry.getValue());
        time = t;
        lastValue = entry.getValue();
    }

    protected abstract void addNewValue(int time, Slice value);

}
