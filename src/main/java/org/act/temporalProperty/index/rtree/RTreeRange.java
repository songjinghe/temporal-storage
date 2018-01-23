package org.act.temporalProperty.index.rtree;

import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceOutput;

import java.util.List;

public class RTreeRange
{
    private final IndexEntryOperator op;
    private Slice min;
    private Slice max;

    public RTreeRange(Slice min, Slice max, IndexEntryOperator op) {
        this.min = min;
        this.max = max;
        this.op = op;
    }

    public RTreeRange(List<Slice> entries, IndexEntryOperator op) {
        this.op = op;
        Slice[] minMax = op.calcMinMax(entries);
        this.min = minMax[0];
        this.max = minMax[1];
    }

    public Slice getMin() {
        return min;
    }

    public void setMin(Slice min) {
        this.min = min;
    }

    public Slice getMax() {
        return max;
    }

    public void setMax(Slice max) {
        this.max = max;
    }

    public boolean overlap(RTreeRange bound) {
        for(int i=0;i<op.dimensionCount();i++){
            if(!overlap(bound, i)) return false;
        }
        return true;
    }

    public boolean contains(Slice point) {
        for(int i = 0; i< this.op.dimensionCount(); i++){
            if(!(op.compare(min, point, i)<=0 && op.compare(point, max, i)<=0)){
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "RTreeRange{" +
                "min=" + op.toString(min) +
                ", max=" + op.toString(max) +
                '}';
    }

    public boolean overlap(RTreeRange bound, int dimIndex) {
        return  op.compare(this.min, bound.max, dimIndex)<=0 &&
                op.compare(bound.min, this.max, dimIndex)<=0;
    }

    public void encode(SliceOutput out){
        out.writeInt(getMin().length());
        out.writeBytes(getMin());
        out.writeInt(getMax().length());
        out.writeBytes(getMax());
    }

}
