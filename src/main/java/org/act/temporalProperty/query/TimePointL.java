package org.act.temporalProperty.query;


/**
 * Created by song on 2018-05-09.
 */
public class TimePointL implements TPoint<TimePointL>
{
    private static final long INIT = -1;
    private static final long NOW = Long.MAX_VALUE;
    public static final TimePointL Now = new TimePointL( NOW );
    public static final TimePointL Init = new TimePointL( -1 );

    private long time;

    public TimePointL( long time )
    {
        this.time = time;
    }

    @Override
    public TimePointL pre()
    {
        return new TimePointL( time - 1 );
    }

    @Override
    public TimePointL next()
    {
        return new TimePointL( time + 1 );
    }

    @Override
    public boolean isNow()
    {
        return time == NOW;
    }

    @Override
    public boolean isInit()
    {
        return time == INIT;
    }



    public long val()
    {
        return time;
    }

    @Override
    public int compareTo( TimePointL o )
    {
        return Long.compare( time, o.time );
    }
}
