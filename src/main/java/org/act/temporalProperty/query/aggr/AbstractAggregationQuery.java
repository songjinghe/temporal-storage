package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Users who want custom aggregation query should extend this class, rather than implement AggregationQuery interface
 * Created by song on 2018-04-01.
 */
public abstract class AbstractAggregationQuery<K,V> extends IntervalAggregationQuery<K,V> {
    private Map<K, V> groupValMap = new HashMap<>();
    private Map<K, List<Entry<Integer, Slice>>> groupListMap = new HashMap<>();

    @Override
    protected void addNewValue(int time, Slice value) {
        K groupId = computeGroupId(time, value);
        if(!groupListMap.containsKey(groupId)) {
            groupListMap.put(groupId, new ArrayList<>());
        }
        groupListMap.get(groupId).add(new TimeValueEntry(time, value));
    }

    @Override
    public Object onReturn() {
        for(Entry<K, List<Entry<Integer, Slice>>> entry : groupListMap.entrySet()){
            groupValMap.put(entry.getKey(), aggregate(entry.getKey(), entry.getValue()));
        }
        return onResult(groupValMap);
    }



}
