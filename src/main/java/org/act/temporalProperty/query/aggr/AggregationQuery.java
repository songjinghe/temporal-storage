package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.util.Slice;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by song on 2018-04-01.
 */
public interface AggregationQuery<K,V> {
    int computeStartTime();
    int computeNextTime(int cur);
    K computeGroupId(int time, Slice value);
    V aggregate(K groupId, Collection<Entry<Integer, Slice>> groupItems);
    Object onResult(Map<K,V> result);

    class TimeValueEntry implements Entry<Integer, Slice>{
        private int time;
        private Slice value;

        public TimeValueEntry(int time, Slice value) {
            this.time = time;
            this.value = value;
        }

        @Override
        public Integer getKey() {
            return time;
        }

        @Override
        public Slice getValue() {
            return value;
        }

        @Override
        public Slice setValue(Slice value) {
            throw new UnsupportedOperationException();
        }
    }
}
