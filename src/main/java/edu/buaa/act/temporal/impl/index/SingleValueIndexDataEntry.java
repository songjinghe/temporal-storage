package edu.buaa.act.temporal.impl.index;

import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.exception.TPSNHException;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.ValueOperator;

import java.util.Comparator;

/**
 * Created by song on 2018-01-09.
 */
public class SingleValueIndexDataEntry extends IndexDataEntry<SingleValueIndexDataEntry>
{
    private ValueAtTime value;
    private final ValueOperator cp;

    public SingleValueIndexDataEntry(TimePoint start, TimePoint end, long entityId, ValueAtTime value, ValueOperator valueCp)
    {
        super(start, end, entityId);
        this.value = value;
        this.cp = valueCp;
    }


    @Override
    public int compareTo(SingleValueIndexDataEntry o, int dimIndex)
    {
        if(dimIndex==0){
            return super.compareStart(o);
        }else if(dimIndex==1){
            return super.compareEnd(o);
        }else if(dimIndex==2){
            return super.compareEntityId(o);
        }else if(dimIndex==3){
            return cp.compare(value, o.value);
        }else{
            throw new TPSNHException("dimIndex too large");
        }
    }


    @Override
    public int dimCount()
    {
        return super.dimCount()+this.valueCount();
    }

    @Override
    public int valueCount()
    {
        return 1;
    }

    @Override
    public void setToAvg(SingleValueIndexDataEntry minBound, SingleValueIndexDataEntry maxBound)
    {
        this.start = minBound.start.avg(maxBound.start);
        this.end = minBound.end.avg(maxBound.end);
        this.entityId = (minBound.entityId+maxBound.entityId)/2;
        this.value = cp.avg(minBound.value, maxBound.value);
    }

    @Override
    public void updateMin(SingleValueIndexDataEntry value)
    {
        if(value.start.compareTo(this.start)<0){
            this.start = value.start;
        }
        if(value.end.compareTo(this.end)<0){
            this.end = value.end;
        }
        if(value.entityId<this.entityId){
            this.entityId = value.entityId;
        }
        if(cp.compare(value.value, this.value)<0){
            this.value = value.value;
        }
    }

    @Override
    public void updateMax(SingleValueIndexDataEntry value)
    {
        if(value.start.compareTo(this.start)>0){
            this.start = value.start;
        }
        if(value.end.compareTo(this.end)>0){
            this.end = value.end;
        }
        if(value.entityId>this.entityId){
            this.entityId = value.entityId;
        }
        if(cp.compare(value.value, this.value)>0) {
            this.value = value.value;
        }
    }
}
