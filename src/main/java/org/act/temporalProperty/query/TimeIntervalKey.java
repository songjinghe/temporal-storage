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

    public TimeIntervalKey( InternalKey start )
    {
        super( start.getStartTime() );
        this.key = start;
    }

    public TimeIntervalKey( InternalKey key, long newStart, long end )
    {
        super( newStart, end );
        this.key = key;
    }

    public InternalKey getStartKey()
    {
        if ( from() == key.getStartTime() )
        {
            return key;
        }
        else
        {
            return new InternalKey( key.getId(), Math.toIntExact( from() ), key.getValueType() );
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
        return from() == that.from();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( from() );
    }

    public TimeIntervalKey changeEnd( long newEnd )
    {
        return new TimeIntervalKey( this.key, from(), newEnd );
    }

    public TimeIntervalKey changeStart( long newStart )
    {
        return new TimeIntervalKey( this.key, newStart, to() );
    }

    public InternalKey getEndKey()
    {
        return new InternalKey( key.getId(), Math.toIntExact( to() + 1 ), ValueType.UNKNOWN );
    }

    @Override
    public String toString()
    {
        return "TimeIntervalKey{start=" + from() + ", end=" + to() + ", pro=" + key.getPropertyId() + ", eid=" + key.getEntityId() + ", type=" +
                key.getValueType() + '}';
    }

    public boolean lessThan( int time )
    {
        return this.lessThan( new TimePointL( time ) );
    }

    public boolean greaterOrEq( int time )
    {
        return this.greaterOrEq( new TimePointL( time ) );
    }

    public boolean span( int minTime, int maxTime )
    {
        return this.span( new TimePointL( minTime ), new TimePointL( maxTime ) );
    }

    public boolean span( int time )
    {
        return this.span( new TimePointL( time ) );
    }

    public boolean between( int min, int maxTime )
    {
        return this.between( new TimePointL( min ), new TimePointL( maxTime ) );
    }
}
