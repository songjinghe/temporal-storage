package org.act.temporalProperty.query;

import com.google.common.base.Preconditions;

import java.util.Objects;

import static org.act.temporalProperty.TemporalPropertyStore.NOW;

/**
 * Created by song on 2018-05-09.
 */
public class TimeInterval extends TInterval<TimePointL>
{
    private long from;
    private long to;

    public TimeInterval( long startTime, long endTime )
    {
        Preconditions.checkArgument( startTime <= endTime );
        this.from = startTime;
        this.to = endTime;
    }

    public long start()
    {
        return from;
    }

    public long end()
    {
        return to;
    }

    public boolean lessThan( int time )
    {
        return time > to;
    }

    public boolean greaterOrEq( int time )
    {
        return from >= time;
    }

    public boolean span( int time )
    {
        return from < time && time <= to;
    }

    public boolean span( int start, int end )
    {
        return this.from < start && start <= end && end <= this.to;
    }

    public boolean between( int start, int end )
    {
        return start <= this.from && this.to <= end;
    }

    public boolean toNow()
    {
        return to == NOW;
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
        return from == that.from;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( from );
    }

    @Override
    public String toString()
    {
        return "TimeInterval{start=" + from + ", end=" + to + '}';
    }

}
