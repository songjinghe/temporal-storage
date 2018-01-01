package edu.buaa.act.temporal;

import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.helper.IOCoder;

import java.util.Comparator;

/**
 * Created by song on 17-12-5.
 *
 * satisfy following constrains: see TimeInterval#checkConstrain() for details
 *
  */
public class TimeInterval implements Comparable<TimeInterval>
{
    private static Comparator<TimePoint> cp = TimePoint.ComparatorASC;
    private TimePoint start;
    private TimePoint end;

    public TimeInterval(TimePoint start, TimePoint end)
    {
        checkConstrains(start, end);
        this.start = start;
        this.end = end;
    }

    private void checkConstrains(TimePoint start, TimePoint end)
    {
        if(start.isInit() && end.isInit()){
            throw new TPSRuntimeException("time interval(init, init) is not allowed");
        }
        if(start.isNow() && end.isNow()){
            throw new TPSRuntimeException("time interval(now, now) is not allowed");
        }
        if(start.compareTo(end)>0){
            throw new TPSRuntimeException("start must not after end");
        }
    }

    public TimePoint getEnd() {
        return end;
    }

    public TimePoint getStart() {
        return start;
    }

    public void setStart(TimePoint start) {
        checkConstrains(start, end);
        this.start = start;
    }

    public TimeInterval copy(){
        return new TimeInterval(start.copy(), end.copy());
    }

    public void setEnd(TimePoint end) {
        checkConstrains(start, end);
        this.end = end;
    }

    public boolean contains(TimePoint time) {
        return (cp.compare(start, time) <= 0 && cp.compare(time, end) <= 0 );
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
