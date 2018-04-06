package org.act.temporalProperty.query.aggr;

import org.act.temporalProperty.table.UserComparator;
import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-04-05.
 */
public class AggregationIndexKey implements Comparable<AggregationIndexKey>{
    private long entityId;
    private int timeGroupId;
    private int valueGroupId;

    public AggregationIndexKey(Slice raw){
        entityId = raw.getLong(0);
        timeGroupId = raw.getInt(8);
        valueGroupId = raw.getInt(12);
    }

    public AggregationIndexKey(long entityId, int timeGroupId, int valueGroupId){
        this.entityId = entityId;
        this.timeGroupId = timeGroupId;
        this.valueGroupId = valueGroupId;
    }

    public long getEntityId() {
        return entityId;
    }

    public int getTimeGroupId() {
        return timeGroupId;
    }

    public int getValueGroupId() {
        return valueGroupId;
    }

    public Slice encode(){
        Slice raw = new Slice(16);
        raw.setLong(0, entityId);
        raw.setInt(8, timeGroupId);
        raw.setInt(12, valueGroupId);
        return raw;
    }

    public static UserComparator sliceComparator = new UserComparator() {
        @Override
        public String name() {
            return "(entity-id, time-group-id, value-group-id)";
        }
        public Slice findShortestSeparator(Slice start, Slice limit) {
            throw new UnsupportedOperationException();
        }
        public Slice findShortSuccessor(Slice key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compare(Slice o1, Slice o2) {
            return new AggregationIndexKey(o1).compareTo(new AggregationIndexKey(o2));
        }
    };

    @Override
    public int compareTo(AggregationIndexKey o) {
        int r = Long.compare(this.entityId, o.entityId);
        if(r==0){
            r = Integer.compare(this.timeGroupId, o.timeGroupId);
            if(r==0){
                return Integer.compare(this.valueGroupId, o.valueGroupId);
            }else{
                return r;
            }
        }else{
            return r;
        }
    }
}
