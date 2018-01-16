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
 *
 * NOW == NOW, INIT == INIT
 */
public class TimePoint implements Comparable<TimePoint>
{
    public static final TimePoint NOW = new TimePoint(-1, true);

    public static final TimePoint INIT = new TimePoint(-2, true);

    public static final IOCoder<TimePoint> IO = new IOCoder<TimePoint>()
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
        public TimePoint decode(ByteBuffer in, int start)
        {
            return new TimePoint(in.getInt(start));
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

    public static final Comparator<TimePoint> ComparatorASC = new Comparator<TimePoint>()
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
                return Integer.compare(o1.time, o2.time); // NOW == NOW, INIT == INIT
            }
        }
    };



    private final int time;

    public TimePoint(long timestamp)
    {
        if(0>timestamp || timestamp>Integer.MAX_VALUE)
        {
            throw new TPSRuntimeException("timestamp should between 0 and INT.MAX");
        }
        this.time = (int) timestamp;
    }

    private TimePoint(int time, boolean special)
    {
        this.time = time;
    }

    public TimePoint post() {
        if(time<=Integer.MAX_VALUE-1) {
            if(0<time){
                return new TimePoint(time + 1);
            }else{
                throw new TPSRuntimeException("NOW and INIT not support post operation.");
            }
        }else{
            throw new TPSRuntimeException("time overflow.");
        }
    }

    public TimePoint pre() {
        if(time>=1) {
            return new TimePoint(time - 1);
        }else{
            if(0<=time){
                throw new TPSRuntimeException("time underflow.");
            }else{
                throw new TPSRuntimeException("NOW and INIT not support pre operation.");
            }
        }
    }

    public boolean hasPost()
    {
        return time<=Integer.MAX_VALUE-1;
    }

    public boolean hasPre()
    {
        return time>=0;
    }

    public boolean isNormal()
    {
        return hasPre() && hasPost();
    }

    public boolean isInit()
    {
        return this==TimePoint.INIT;
    }

    public boolean isNow()
    {
        return this==TimePoint.NOW;
    }

    public TimePoint copy(){
        return new TimePoint(this.time);
    }

    public byte[] encode(){
        return IO.encode(this);
    }

    @Override
    public int compareTo(TimePoint o)
    {
        return ComparatorASC.compare(this, o);
    }

    public TimePoint avg(TimePoint o)
    {
        if(this.time<0 || o.time<0){
            throw new TPSRuntimeException("NOW and INIT not support avg operator!");
        }
        long tmp = this.time;
        tmp+=o.time;
        tmp/=2;
        return new TimePoint((int) tmp);
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
