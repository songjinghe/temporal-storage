package org.act.temporalProperty.index.rtree;


import com.google.common.base.Preconditions;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;
import org.act.temporalProperty.util.VariableLengthQuantity;

/**
 * Created by song on 2018-01-21.
 */
public class IndexEntry {
    private final int start;
    private final int end;
    private final Slice[] value;
    private Long entityId = null;

    public IndexEntry(int start, int end, Slice[] value) {
        Preconditions.checkArgument(start <= end, "start %s > end %s", start, end);
        this.start = start;
        this.end = end;
        this.value = value;
    }

    public IndexEntry(Slice content) {
        this(content.input());
    }

    public IndexEntry(SliceInput in) {
        byte flag = in.readByte();
        boolean hasEntityId = ( flag < 0 );
        int valueCount = flag<0 ? -flag : flag;
        if(hasEntityId){
            this.entityId = VariableLengthQuantity.readVariableLengthLong(in);
        }
        this.start = in.readInt();
        this.end = in.readInt();
        this.value = new Slice[valueCount];
        for(int i=0; i<valueCount; i++) {
            this.value[i] = nextEntry(in);
        }
    }

    public IndexEntry(long entityId, int startTime, int endTime, Slice[] values) {
        this(startTime, endTime, values);
        this.entityId = entityId;
    }

    private Slice nextEntry(SliceInput in){
        int len = VariableLengthQuantity.readVariableLengthInt(in);
        if(len==0) return null;
        else return in.readSlice(len);
    }

    private void putNextEntry(SliceOutput out, Slice entry) {
        if(entry==null){
            VariableLengthQuantity.writeVariableLengthInt(0, out);
        }else{
            VariableLengthQuantity.writeVariableLengthInt(entry.length(), out);
            out.writeBytes(entry);
        }
    }

    public long getEntityId() {
        if(entityId!=null) return entityId;
        else throw new RuntimeException("do not contains entityId");
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public Slice getValue(int i){
        return value[i];
    }

    public void encode(SliceOutput out, boolean withEntityId) {
        if(withEntityId){
            out.writeByte(0-value.length);
            VariableLengthQuantity.writeVariableLengthLong(entityId, out);
        }else{
            out.writeByte(value.length);
        }
        out.writeInt(start);
        out.writeInt(end);
        for(Slice entry : value){
            putNextEntry(out, entry);
        }
    }

    public void encode(SliceOutput out) {
        encode(out, false);
    }

    public Slice encode(boolean withEntityId) {
        DynamicSliceOutput out = new DynamicSliceOutput(64);
        encode(out, withEntityId);
        return out.slice();
    }

    public Slice encode() { return encode(false); }
}
