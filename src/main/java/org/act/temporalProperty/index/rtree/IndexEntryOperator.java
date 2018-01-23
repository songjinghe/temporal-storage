package org.act.temporalProperty.index.rtree;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.index.IndexIntervalEntry;
import org.act.temporalProperty.index.IndexQueryRegion;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.PropertyValueInterval;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;

import java.util.List;

/**
 * Created by song on 2018-01-19.
 *
 * IndexEntry is a byte array (Slice), which contains following fields:
 * entity id : long, 8 bytes
 * start time: int, 4 bytes --- dimIndex 0
 * end time  : int, 4 bytes --- dimIndex 1
 * value1    : ?, ?   bytes --- dimIndex 2 --- if it is a string, then its length is contained in value1
 * ...
 */
public class IndexEntryOperator {

    private static final int defaultIndexFieldCount = 2; //start, end. not include entity id because it is only useful in IndexDataEntry
    private final List<IndexValueType> values;
    private final int dataBlockCapacity;
    private final int indexBlockCapacity;

    public IndexEntryOperator(List<IndexValueType> valueTypeList, int blockSize){
        this.values = valueTypeList;
        int size = estimateIndexEntrySize(valueTypeList);
        this.dataBlockCapacity = blockSize / (size+8);//8 for entity id;
        this.indexBlockCapacity = blockSize / (size+8);
    }

    private int estimateIndexEntrySize(List<IndexValueType> valueTypeList) {
        int size = 0;
        for(IndexValueType type : valueTypeList){
            switch (type){
                case INT: size+=4;break;
                case LONG:size+=8;break;
                case FLOAT:size+=4;break;
                case DOUBLE:size+=8;break;
                case STRING:size+=32;break;
            }
        }
        return size;
    }

    public int dimensionCount() {
        return values.size()+ defaultIndexFieldCount;
    }

    public int compare(Slice entry1, Slice entry2, int dimIndex) {
        IndexValueType type = getType(dimIndex);
        return type.compare(getField(dimIndex, entry1), getField(dimIndex, entry2));
    }

    private IndexValueType getType(int dimIndex){
        switch (dimIndex){
            case 0:
            case 1: return IndexValueType.INT;
            default:
                Preconditions.checkArgument(defaultIndexFieldCount <=dimIndex && dimIndex<dimensionCount());
                int customValIndex = dimIndex - defaultIndexFieldCount;
                return values.get(customValIndex);
        }
    }

    private Slice getField(int dimIndex, Slice entry){
        switch (dimIndex){
            case 0: return entry.slice(8,4);//start
            case 1: return entry.slice(12, 4);//end
            default:
            {
                Preconditions.checkArgument(defaultIndexFieldCount <=dimIndex && dimIndex<dimensionCount());
                int customValIndex = dimIndex - defaultIndexFieldCount;
                int readStart=16; // 16=8(entity id)+4(start time)+4(end time)
                for(int i=0; i<customValIndex; i++){
                    IndexValueType type = values.get(i);
                    switch (type){
                        case STRING:
                            readStart += (4+entry.getInt(readStart));// str length;
                            break;
                        case INT:
                        case FLOAT:
                            readStart += 4;
                        case LONG:
                        case DOUBLE:
                            readStart += 8;
                    }
                }
                IndexValueType type = values.get(customValIndex);
                switch (type){
                    case INT:
                    case FLOAT:
                        return entry.slice(readStart, 4);
                    case LONG:
                    case DOUBLE:
                        return entry.slice(readStart, 8);
                    case STRING:
                    default:
                        int len = entry.getInt(readStart);
                        return entry.slice(readStart+4, len);
                }
            }
        }
    }

    public int compareRange(RTreeRange bound1, RTreeRange bound2, int dimIndex) {
        if(!bound1.overlap(bound2, dimIndex)){
            return compare(bound1.getMin(), bound2.getMin(), dimIndex);
        }else{
            Slice min1 = getField(dimIndex, bound1.getMin().slice());
            Slice max1 = getField(dimIndex, bound1.getMax().slice());
            Slice min2 = getField(dimIndex, bound2.getMin().slice());
            Slice max2 = getField(dimIndex, bound2.getMax().slice());
            return getType(dimIndex).compareRange(min1, max1, min2, max2);
        }
    }

    public long getEntityId(Slice entry){
        return entry.getLong(0);
    }

    public int dataBlockCapacity() {
        return dataBlockCapacity;
    }

    public int indexBlockCapacity() {
        return indexBlockCapacity;
    }

    public RTreeRange toRTreeRange(IndexQueryRegion regions) {
        DynamicSliceOutput min = new DynamicSliceOutput(64);
        min.writeLong(0); // entity id, however it is no use, only as placeholder
        min.writeInt(0); // start time, time for minStart, minEnd, maxStart, maxEnd needs special care.
        min.writeInt(regions.getTimeMin());// end time

        DynamicSliceOutput max = new DynamicSliceOutput(64);
        max.writeLong(Long.MAX_VALUE);  // entity id, however it is no use, only as placeholder
        max.writeInt(regions.getTimeMax()); // start time
        max.writeInt(Integer.MAX_VALUE);// end time

        List<PropertyValueInterval> vList =  regions.getPropertyValueIntervals();
        for(int i=0; i<vList.size(); i++){
            PropertyValueInterval p = vList.get(i);
            IndexValueType type = p.getType();
            if(this.values.get(i)!= type) throw new RuntimeException("incorrect value order");
            min.writeBytes(p.getValueMin());
            max.writeBytes(p.getValueMax());
        }
        return new RTreeRange(min.slice(), max.slice(), this);
    }

    public Slice toSlice(IndexIntervalEntry entry) {
        DynamicSliceOutput out = new DynamicSliceOutput(20);
        out.writeLong(entry.getEntityId());
        out.writeInt(entry.getStart());
        out.writeInt(entry.getEnd());
        out.writeBytes(entry.getValue());
        return out.slice();
    }

    public Slice[] calcMinMax(List<Slice> entries) {
        DynamicSliceOutput min = new DynamicSliceOutput(20);
        DynamicSliceOutput max = new DynamicSliceOutput(20);
        min.writeLong(0); // entity id, however it is no use, only as placeholder
        max.writeLong(Long.MAX_VALUE); // entity id, however it is no use, only as placeholder
        for(int i=0;i<dimensionCount();i++){
            Slice[] minmax = calcMinMaxField(entries, i);
            min.writeBytes(minmax[0]);
            max.writeBytes(minmax[1]);
        }
        return new Slice[]{min.slice(), max.slice()};
    }

    private Slice[] calcMinMaxField(List<Slice> entries, int dimIndex) {
        IndexValueType type = getType(dimIndex);
        Slice min=null, max=null;
        for(int i=0; i<entries.size(); i++){
            Slice entry = entries.get(i);
            if(i==0){
                min=max=getField(dimIndex, entry);
            }else {
                Slice field = getField(dimIndex, entry);
                if(type.compare(field, min)<0) min = field;
                if(type.compare(field, max)>0) max = field;
            }
        }
        return new Slice[]{min, max};
    }

    public String toString(Slice entry) {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexEntity(eid=").append(entry.getLong(0)).append(",");
        sb.append("start=").append(entry.getInt(8)).append(",");
        sb.append("end=").append(entry.getInt(12)).append(",");
        if(entry.length()>=20) {
            sb.append("val1=").append(entry.getInt(16)).append(")");
        }else{
            sb.append("val1=").append("?").append(")");
        }
        return sb.toString();
    }
}
