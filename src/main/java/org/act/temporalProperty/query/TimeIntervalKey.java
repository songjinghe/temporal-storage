package org.act.temporalProperty.query;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;

import static org.act.temporalProperty.TemporalPropertyStore.NOW;

/**
 * Created by song on 2018-05-05.
 */
public class TimeIntervalKey
{
    private InternalKey key;
    //later the start and end may not sync with initial value.
    private long start;
    private long end;

    public TimeIntervalKey( InternalKey start, long end )
    {
        this.key = start;
        this.start = start.getStartTime();
        this.end = end;
        Preconditions.checkArgument( this.start<=end );
    }

    private TimeIntervalKey( InternalKey key, long newStart, long end )
    {
        Preconditions.checkArgument( newStart<=end );
        this.key = key;
        this.start = newStart;
        this.end = end;
    }

    public InternalKey getStartKey()
    {
        if ( start == key.getStartTime() )
        {
            return key;
        }
        else
        {
            return new InternalKey( key.getId(), Math.toIntExact( start ), key.getValueType() );
        }
    }

    public long getStart()
    {
        return start;
    }

    public InternalKey getKey()
    {
        return key;
    }

    public long getEnd()
    {
        return end;
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
        return start == that.start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( start );
    }

    public TimeIntervalKey changeEnd( long newEnd )
    {
        return new TimeIntervalKey( this.key, this.start, newEnd );
    }

    public TimeIntervalKey changeStart( long newStart )
    {
        return new TimeIntervalKey( this.key, newStart, this.end );
    }

    public InternalKey getEndKey()
    {
        return new InternalKey( key.getId(), Math.toIntExact( end + 1 ), ValueType.UNKNOWN );
    }

    public boolean lessThan( int time )
    {
        return time > end;
    }

    public boolean greaterOrEq( int time )
    {
        return start >= time;
    }

    public boolean span( int time )
    {
        return start < time && time <= end;
    }

    public boolean span( int start, int end )
    {
        return this.start < start && start <= end && end <= this.end;
    }

    public boolean between( int start, int end )
    {
        return start <= this.start && this.end <= end;
    }

    public boolean isEndEqNOW()
    {
        return end == NOW;
    }
}
