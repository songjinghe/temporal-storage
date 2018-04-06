package org.act.temporalProperty.index;

import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by song on 2018-01-17.
 */
public class IndexMetaData {
    private long fileId;
    private IndexType type;
    private List<Integer> propertyIdList;
    private int timeStart;
    private int timeEnd;
    private long fileSize;

    public IndexMetaData(long fileId, IndexType type, List<Integer> pidList, int start, int end, long fileSize) {
        this.fileId = fileId;
        this.type = type;
        this.propertyIdList = pidList;
        this.timeStart = start;
        this.timeEnd = end;
        this.fileSize = fileSize;
    }

    public long getId() {
        return fileId;
    }

    public IndexType getType() {
        return type;
    }

    public int getTimeStart() {
        return timeStart;
    }

    public int getTimeEnd() {
        return timeEnd;
    }

    public List<Integer> getPropertyIdList(){
        return propertyIdList;
    }

    public long getFileSize() {
        return fileSize;
    }

    @Override
    public String toString() {
        return "IndexMetaData{" +
                "fileId=" + fileId +
                ", type=" + type +
                ", propertyIdList=" + propertyIdList +
                ", timeStart=" + timeStart +
                ", timeEnd=" + timeEnd +
                ", fileSize=" + fileSize +
                '}';
    }

    public Slice encode(){
        DynamicSliceOutput out = new DynamicSliceOutput(128);
        encode(out);
        return out.slice();
    }

    public void encode(SliceOutput out){
        out.writeInt(this.getType().getId());
        out.writeInt(this.getTimeStart());
        out.writeInt(this.getTimeEnd());
        out.writeLong(this.getFileSize());
        out.writeInt(this.getPropertyIdList().size());
        for(Integer pid : this.getPropertyIdList()){
            out.writeInt(pid);
        }
    }

    public IndexMetaData(SliceInput in){
        this.fileId = in.readInt();
        this.type = IndexType.decode(in.readInt());
        this.timeStart = in.readInt();
        this.timeEnd = in.readInt();
        this.fileSize = in.readLong();
        int count = in.readInt();
        List<Integer> pidList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            pidList.add(in.readInt());
        }
        this.propertyIdList = pidList;
    }

    public IndexMetaData(Slice in){
        this(in.input());
    }
}
