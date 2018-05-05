package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.util.Slice;

import java.util.TreeMap;

/**
 * Created by song on 2018-05-05.
 */
public class AggregationIndexQueryResult
{

    private TreeMap<Integer,Slice> minMaxResult;
    private TreeMap<Integer,Integer> durationResult;
    private final int speedUpTime;

    public AggregationIndexQueryResult( TreeMap<Integer,Integer> result, int speedUpTime )
    {
        this.durationResult = result;
        this.speedUpTime = speedUpTime;
    }

    public AggregationIndexQueryResult( TreeMap<Integer,Slice> result, int speedUpTime, ValueContentType valueType )
    {
        this.minMaxResult = result;
        this.speedUpTime = speedUpTime;

    }
}
