package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;

/**
 * Created by song on 17-12-27.
 */
public class TimePointValueEntry
{
    private TimePoint time;
    private ValueAtTime value;

    public TimePointValueEntry(TimePoint time, ValueAtTime value)
    {
        this.time = time;
        this.value = value;
    }

    public TimePoint getTime()
    {
        return time;
    }

    public ValueAtTime getValue()
    {
        return value;
    }
}
