package org.act.temporalProperty.query;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Created by song on 2018-05-09.
 */
public class TimeInterval extends TInterval<TimePointL>
{
    public TimeInterval( long startTime, long endTime )
    {
        super( new TimePointL( startTime ), new TimePointL( endTime ) );
    }

    public TimeInterval( long startTime )
    {
        super( new TimePointL( startTime ), TimePointL.Now );
    }

    public TimeInterval( TimePointL startTime, TimePointL endTime )
    {
        super( startTime , endTime );
    }

    public long from()
    {
        return start().val();
    }

    public int fromInt()
    {
        return Math.toIntExact( start().val() );
    }

    public long to()
    {
        return end().val();
    }

    public int toInt()
    {
        return Math.toIntExact( end().val() );
    }

    @Override
    public TInterval<TimePointL> changeEnd( TimePointL newEnd )
    {
        return new TimeInterval( from(), newEnd.val() );
    }

    @Override
    public TInterval<TimePointL> changeStart( TimePointL newStart )
    {
        return new TimeInterval( newStart.val(), to() );
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
        TimeInterval that = (TimeInterval) o;
        return from() == that.from();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( from() );
    }

    @Override
    public String toString()
    {
        return "TimeInterval{start=" + from() + ", end=" + to() + '}';
    }

}
