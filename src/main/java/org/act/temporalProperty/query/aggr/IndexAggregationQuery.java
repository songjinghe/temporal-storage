package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.index.TimeIntervalEntry;
import org.act.temporalProperty.query.range.InternalEntryRangeQueryCallBack;

import java.util.Map;

/**
 * Created by song on 2018-04-05.
 */
public interface IndexAggregationQuery extends InternalEntryRangeQueryCallBack {
    Object onResult(Map<Integer, Integer> valueGroupMap);
}
