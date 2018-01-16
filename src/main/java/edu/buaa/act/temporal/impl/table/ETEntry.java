package edu.buaa.act.temporal.impl.table;

import com.google.common.base.Objects;
import edu.buaa.act.temporal.TimePoint;

import java.nio.ByteBuffer;

/**
 * Created by song on 2018-01-02.
 */
public class ETEntry implements Comparable<ETEntry>
{
    private final long entityId;
    private final TimePoint time;

    public ETEntry(long entityId, TimePoint time)
    {
        this.entityId = entityId;
        this.time = time;
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
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ETEntry etEntry = (ETEntry) o;
        return entityId == etEntry.entityId &&
                Objects.equal(time, etEntry.time);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(entityId, time);
    }

    public int compareTo(ByteBuffer that, int start){
        long eid = that.getLong(start);
        TimePoint t = TimePoint.IO.decode(that, start+8);
        return this.compareTo(new ETEntry(eid, t));
    }

    @Override
    public int compareTo(ETEntry o)
    {
        int cmp = Long.compare(entityId, o.entityId);
        if(cmp==0){
            return time.compareTo(o.time);
        }else{
            return cmp;
        }
    }

}
