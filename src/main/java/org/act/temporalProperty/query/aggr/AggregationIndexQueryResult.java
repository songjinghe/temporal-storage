package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.util.Slice;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by song on 2018-05-05.
 */
public class AggregationIndexQueryResult
{

    private Map<Integer,Slice> minMaxResult;
    private Map<Integer,Integer> durationResult;
    private final int speedUpTime;

    public AggregationIndexQueryResult( Map<Integer,Integer> result, int speedUpTime )
    {
        this.durationResult = result;
        this.speedUpTime = speedUpTime;
    }

    public AggregationIndexQueryResult( Map<Integer,Slice> result, int speedUpTime, ValueContentType valueType )
    {
        this.minMaxResult = result;
        this.speedUpTime = speedUpTime;
    }

    public Map<Integer,Slice> getMinMaxResult()
    {
        return minMaxResult;
    }

    public Map<Integer,Integer> getDurationResult()
    {
        return durationResult;
    }

    public int getAccelerateTime()
    {
        return speedUpTime;
    }
}
