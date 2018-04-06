package org.act.temporalProperty.index;

import com.google.common.collect.Lists;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;
import org.apache.commons.lang3.time.DateUtils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by song on 2018-04-06.
 */
public class AggregationIndexMeta extends IndexMetaData {
    private final TreeMap<Slice, Integer> vGroupMap;
    private final int tEvery;
    private final int timeUnit;
    private TreeMap<Integer, Integer> tGroupMap;

    public AggregationIndexMeta(long fileId, IndexType type, int pid, int start, int end, long fileSize,
                                int tEvery, int timeUnit, TreeMap<Slice, Integer> valueGroup) {
        super(fileId, type, Lists.newArrayList(pid), start, end, fileSize);
        this.vGroupMap = valueGroup;
        this.tEvery = tEvery;
        this.timeUnit = timeUnit;
    }

    public TreeMap<Slice, Integer> getValGroupMap() {
        return vGroupMap;
    }

    public TreeMap<Integer, Integer> getTimeGroupMap() {
        if(tGroupMap==null){
            tGroupMap = calcInterval(getTimeStart(), getTimeEnd(), tEvery, timeUnit);
        }
        return tGroupMap;
    }

    @Override
    public String toString() {
        return "AggregationIndexMeta{" +
                "fileId=" + getId() +
                ", type=" + getType() +
                ", propertyId=" + getPropertyIdList().get(0) +
                ", timeStart=" + getTimeStart() +
                ", timeEnd=" + getTimeEnd() +
                ", fileSize=" + getFileSize() +
                '}';
    }


    public Slice encode(){
        DynamicSliceOutput out = new DynamicSliceOutput(128);
        encode(out);
        return out.slice();
    }

    public void encode(SliceOutput out){
        super.encode(out);
        out.writeInt(tEvery);
        out.writeInt(timeUnit);
        out.writeInt(vGroupMap.size());
        for(Map.Entry<Slice, Integer> entry : vGroupMap.entrySet()){
            out.writeInt(entry.getKey().length());
            out.writeBytes(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    public static AggregationIndexMeta decode(SliceInput in){
        int fileId = in.readInt();
        IndexType type = IndexType.decode(in.readInt());
        int timeStart = in.readInt();
        int timeEnd = in.readInt();
        long fileSize = in.readLong();
        int count = in.readInt();
        List<Integer> pidList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            pidList.add(in.readInt());
        }
        int tEvery = in.readInt();
        int timeUnit = in.readInt();
        TreeMap<Slice, Integer> valGroupMap = new TreeMap<>();
        count = in.readInt();
        for(int i=0; i<count; i++){
            int len = in.readInt();
            Slice key = in.readBytes(len);
            int groupId = in.readInt();
            valGroupMap.put(key, groupId);
        }
        return new AggregationIndexMeta(fileId, type, pidList.get(0), timeStart, timeEnd, fileSize, tEvery, timeUnit, valGroupMap);
    }

    public static AggregationIndexMeta decode(Slice in){
        return decode(in.input());
    }

    public static TreeMap<Integer, Integer> calcInterval(int start, int end, int every, int timeUnit) {
        TreeMap<Integer, Integer> set = new TreeMap<>();
        int timeGroupId = 0;
        Calendar cur = Calendar.getInstance();
        cur.setTimeInMillis(start*1000);
        // set `iStart` to the smallest interval discrete time, which larger than `cur`.
        Calendar iStart = DateUtils.round(cur, timeUnit);// be careful with `daylight saving time`, check doc.
        while(iStart.getTimeInMillis()<=end*1000) {
            set.put((int) (iStart.getTimeInMillis()/1000), timeGroupId);
            timeGroupId++;
            iStart.add(timeUnit, every);
        }
        return set;
    }

}
