package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.query.range.InternalEntryRangeQueryCallBack;
import org.act.temporalProperty.util.Slice;

import java.util.Map;

/**
 * Created by song on 2018-04-05.
 */
public interface IndexAggregationQuery extends InternalEntryRangeQueryCallBack {

    interface MinMax extends IndexAggregationQuery{
        int MIN=0;
        int MAX=1;

        /**
         *
         * @param valueGroupMap     结果集, get(MinMax.MIN)得到最小值, get(MinMax.MAX)得最大值. 若index中只定义了MIN,查询MAX为null
         * @param indexQueryOverlap 使用索引进行加速的时间区间长度, 单位秒
         * @return
         */
        Object onResult(Map<Integer, Slice> valueGroupMap, int indexQueryOverlap);
    }

    interface Duration extends IndexAggregationQuery{
        Object onResult(Map<Integer, Integer> valueGroupMap, int indexQueryOverlap);
    }

}
