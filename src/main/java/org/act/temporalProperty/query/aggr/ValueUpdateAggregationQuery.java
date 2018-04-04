package org.act.temporalProperty.query.aggr;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.util.Slice;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
/**
 * Created by song on 2018-04-02.
 */
public abstract class ValueUpdateAggregationQuery<K, V> extends IntervalAggregationQuery<K, V> {
    private final Map<K, V> groupItemCountMap = new HashMap<>();

    @Override
    protected void addNewValue(int t, Slice value){
        K groupId = computeGroupId(t, value);
//        groupItemCountMap.merge(groupId, 1, (oldValue, one) -> oldValue + one);
        if(!groupItemCountMap.containsKey(groupId)) {
            groupItemCountMap.put(groupId, init(value));
        }else {
            groupItemCountMap.put(groupId, update(groupItemCountMap.get(groupId), value));
        }
    }

    @Override
    public Object onReturn() {
        return onResult(groupItemCountMap);
    }

    @Override
    public V aggregate(K groupId, Collection<Map.Entry<Integer, Slice>> groupItems) {
        throw new UnsupportedOperationException();//no need to implement.
    }

    protected int asInt(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=4);
        return value.getInt(0);
    }

    protected float asFloat(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=4);
        return value.getFloat(0);
    }

    protected long asLong(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=8);
        return value.getLong(0);
    }

    protected double asDouble(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=8);
        return value.getDouble(0);
    }

    protected int min(int old, int value){
        return Math.min(old, value);
    }

    protected abstract V update(V currentAggrValue, Slice value);
    protected abstract V init(Slice value);

}