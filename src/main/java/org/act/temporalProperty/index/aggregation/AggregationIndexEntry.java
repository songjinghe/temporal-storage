package org.act.temporalProperty.index.aggregation;

import org.act.temporalProperty.query.aggr.AggregationIndexKey;

import java.util.Map;

/**
 * Created by song on 2018-04-05.
 */
public class AggregationIndexEntry implements Map.Entry<AggregationIndexKey, Integer> {
    private final AggregationIndexKey key;
    private int duration;

    public AggregationIndexEntry(AggregationIndexKey key) {
        this.key = key;
        this.duration = 0;
    }

    public AggregationIndexEntry(AggregationIndexKey key, int duration){
        this.key = key;
        this.duration = duration;
    }

    public int getDuration(){
        return duration;
    }

    public AggregationIndexKey getKey(){
        return key;
    }

    @Override
    public Integer getValue() {
        return getDuration();
    }

    @Override
    public Integer setValue(Integer value) {
        int old = duration;
        this.duration = value;
        return old;
    }
}
