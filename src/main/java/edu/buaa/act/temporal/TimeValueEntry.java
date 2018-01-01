package edu.buaa.act.temporal;

/**
 * Created by song on 17-12-5.
 */
public class TimeValueEntry
{
    private TimeInterval time;
    private ValueAtTime value;

    public TimeValueEntry(TimeInterval time, ValueAtTime value)
    {
        this.time = time;
        this.value = value;
    }

    public TimeInterval getTime() {
        return time;
    }

    public TimeValueEntry copy()
    {
        return new TimeValueEntry(time.copy(), value);
    }


    public ValueAtTime getValue() {
        return value;
    }
}
