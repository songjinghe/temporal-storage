package edu.buaa.act.temporal.impl.index;

import edu.buaa.act.temporal.TimePoint;

/**
 * Created by song on 2018-01-09.
 */
public abstract class IndexDataEntry<T extends IndexDataEntry>
{
    protected TimePoint start;
    protected TimePoint end;
    protected long entityId;

    public IndexDataEntry(TimePoint start, TimePoint end, long entityId){
        this.start = start;
        this.end = end;
        this.entityId = entityId;
    }

    protected int compareStart(IndexDataEntry o){
        return start.compareTo(o.start);
    }

    protected int compareEnd(IndexDataEntry o){
        return end.compareTo(o.end);
    }

    protected int compareEntityId(IndexDataEntry o){
        return Long.compare(entityId, o.entityId);
    }

    public abstract int compareTo(T o, int dimIndex);

    public int dimCount(){
        return 3;
    }

    public abstract int valueCount();

    public abstract void setToAvg(T minBound, T maxBound);

    public abstract void updateMin(T value);

    public abstract void updateMax(T value);
}
