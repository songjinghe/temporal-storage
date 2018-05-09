package org.act.temporalProperty.query;

import com.google.common.base.Objects;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.ValueType;

/**
 * Created by song on 2018-05-05.
 */
public class TimeIntervalKey extends TimeInterval
{
    private InternalKey key;
    //later the start and end may not sync with initial value.

    public TimeIntervalKey( InternalKey start, long end )
    {
        super( start.getStartTime(), end );
        this.key = start;
    }

    private TimeIntervalKey( InternalKey key, long newStart, long end )
    {
        super( newStart, end );
        this.key = key;
    }

    public InternalKey getStartKey()
    {
        if ( start() == key.getStartTime() )
        {
            return key;
        }
        else
        {
            return new InternalKey( key.getId(), Math.toIntExact( start() ), key.getValueType() );
        }
    }

    public InternalKey getKey()
    {
        return key;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TimeIntervalKey that = (TimeIntervalKey) o;
        return start() == that.start();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( start() );
    }

    public TimeIntervalKey changeEnd( long newEnd )
    {
        return new TimeIntervalKey( this.key, start(), newEnd );
    }

    public TimeIntervalKey changeStart( long newStart )
    {
        return new TimeIntervalKey( this.key, newStart, end() );
    }

    public InternalKey getEndKey()
    {
        return new InternalKey( key.getId(), Math.toIntExact( end() + 1 ), ValueType.UNKNOWN );
    }

    @Override
    public String toString()
    {
        return "TimeIntervalKey{start=" + start() + ", end=" + end() + ", pro=" + key.getPropertyId() + ", eid=" + key.getEntityId() + ", type=" +
                key.getValueType() + '}';
    }
}
