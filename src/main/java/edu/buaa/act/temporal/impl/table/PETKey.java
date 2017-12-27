package edu.buaa.act.temporal.impl.table;

import edu.buaa.act.temporal.TimePoint;

/**
 * Created by song on 17-12-27.
 */
public class PETKey implements Comparable<PETKey>
{
    private int propertyId;
    private long entityId;
    private TimePoint time;

    public PETKey(int propertyId, long entityId, TimePoint time)
    {
        this.propertyId = propertyId;
        this.entityId = entityId;
        this.time = time;
    }

    public int getPropertyId()
    {
        return propertyId;
    }

    public long getEntityId()
    {
        return entityId;
    }

    public TimePoint getTime()
    {
        return time;
    }

    @Override
    public int compareTo(PETKey o)
    {
        int p = Integer.compare(propertyId, o.propertyId);
        if(p!=0){
            return p;
        }else{
            int e = Long.compare(entityId, o.entityId);
            if(e!=0){
                return e;
            }else{
                return time.compareTo(o.time);
            }
        }
    }
}
