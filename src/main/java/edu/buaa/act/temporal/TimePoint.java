package edu.buaa.act.temporal;

import com.google.common.base.Objects;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.primitives.Ints;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.helper.IOCoder;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.Comparator;

/**
 * Created by song on 17-12-5.
 */
public class TimePoint implements Comparable<TimePoint>
{
    public static TimePoint NOW = new TimePoint(-1);

    public static TimePoint INIT = new TimePoint(-2);

    public static IOCoder<TimePoint> IO = new IOCoder<TimePoint>()
    {
        @Override
        public int rawSize()
        {
            return 4;
        }

        @Override
        public TimePoint decode(byte[] src)
        {
            return new TimePoint(Ints.fromByteArray(src));
        }

        @Override
        public TimePoint decode(DataInput in) throws IOException
        {
            return new TimePoint(in.readInt());
        }

        @Override
        public TimePoint decode(ByteBuffer in)
        {
            return new TimePoint(in.getInt());
        }

        @Override
        public byte[] encode(TimePoint timePoint)
        {
            return Ints.toByteArray(timePoint.time);
        }
    };

    public static Comparator<TimePoint> ComparatorASC = new Comparator<TimePoint>()
    {
        @Override
        public int compare(TimePoint o1, TimePoint o2)
        {
            if(o1.time>=0 && o2.time>=0)
            {
                return Integer.compare(o1.time, o2.time);
            }else if(o1.time<0 && o2.time>=0){
                if(o1.time==-1) return 1; // o1 is NOW, o2 is not NOW, thus o1>o2
                else return -1; // o1 is INIT, o2 is not INIT, thus o1 < o2
            }else if(o1.time>=0 && o2.time<=0){
                if(o2.time==-1) return -1; // o2 is NOW
                else return 1; // o2 is INIT
            }else{
                return Integer.compare(o1.time, o2.time);
            }
        }
    };



    private int time;

    public TimePoint(int timestamp)
    {
        this.time = timestamp;
    }

    public TimePoint post() {
        if(time<Integer.MAX_VALUE-1) {
            return new TimePoint(time + 1);
        }else{
            throw new TPSRuntimeException("time overflow.");
        }
    }

    public TimePoint pre() {
        if(time>1) {
            return new TimePoint(time - 1);
        }else{
            throw new TPSRuntimeException("time underflow.");
        }
    }

    public TimePoint copy(){
        return new TimePoint(this.time);
    }

    @Override
    public int compareTo(TimePoint o)
    {
        return ComparatorASC.compare(this, o);
    }

    public TimePoint avg(TimePoint o)
    {
        return new TimePoint((this.time+o.time)/2);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimePoint timePoint = (TimePoint) o;
        return time == timePoint.time;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(time);
    }
}
