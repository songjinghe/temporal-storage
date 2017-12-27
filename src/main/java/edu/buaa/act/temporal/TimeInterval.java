package edu.buaa.act.temporal;

import edu.buaa.act.temporal.helper.IOCoder;

import java.util.Comparator;

/**
 * Created by song on 17-12-5.
 */
public class TimeInterval implements Comparable<TimeInterval>
{
    private static Comparator<TimePoint> cp = TimePoint.ComparatorASC;
    private TimePoint start;
    private TimePoint end;

    public TimeInterval(TimePoint start, TimePoint end)
    {
        this.start = start;
        this.end = end;
    }

    public TimePoint getEnd() {
        return end;
    }

    public TimePoint getStart() {
        return start;
    }

    public void setStart(TimePoint start) {
        this.start = start;
    }

    public TimeInterval copy(){
        return new TimeInterval(start.copy(), end.copy());
    }

    public void setEnd(TimePoint end) {
        this.end = end;
    }

    public boolean contains(TimePoint time) {
        return (cp.compare(start, time) <= 0 &&
                cp.compare(time, end) <= 0 );
    }

    public boolean overlap(TimeInterval timeRange) {
        return (cp.compare(start, timeRange.getEnd())<=0 &&
                cp.compare(timeRange.getStart(), end)<=0);
    }

    @Override
    public int compareTo(TimeInterval o)
    {
        return cp.compare(
                start.avg(end),
                o.getStart().avg(o.getEnd()));
    }
}
