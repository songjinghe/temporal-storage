package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.query.range.InternalEntryRangeQueryCallBack;
import org.act.temporalProperty.util.Slice;

import java.util.Map;

/**
 * Created by song on 2018-04-05.
 */
public interface IndexAggregationQuery extends InternalEntryRangeQueryCallBack {

    interface MinMax extends IndexAggregationQuery{
        Object onResult(Map<Integer, Slice> valueGroupMap);
    }

    interface Duration extends IndexAggregationQuery{
        Object onResult(Map<Integer, Integer> valueGroupMap);
    }

}
