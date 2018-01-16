package edu.buaa.act.temporal.impl.table;

import com.google.common.base.Objects;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.exception.TPSRuntimeException;

import java.util.Map;

/**
 * Created by song on 2018-01-01.
 */
public class ETVEntry implements Map.Entry<ETEntry, ValueAtTime>
{
    private long entityId;
    private TimePoint time;
    private ValueAtTime value;

    public ETVEntry(long entityId, TimePoint time, ValueAtTime value)
    {
        this.entityId = entityId;
        this.time = time;
        this.value = value;
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
    public ETEntry getKey()
    {
        return new ETEntry(this.entityId, this.time);
    }

    public ValueAtTime getValue()
    {
        return value;
    }

    @Override
    public ValueAtTime setValue(ValueAtTime value)
    {
        throw new TPSRuntimeException("SNH: immutable object");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ETVEntry etvEntry = (ETVEntry) o;
        return entityId == etvEntry.entityId &&
                Objects.equal(time, etvEntry.time) &&
                Objects.equal(value, etvEntry.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(entityId, time, value);
    }
}
