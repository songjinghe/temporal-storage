package org.act.temporalProperty.index.aggregation;

import com.google.common.collect.Lists;
import org.act.temporalProperty.index.value.IndexMetaData;
import org.act.temporalProperty.index.IndexType;
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

     /**
      * 计算指定时间区间上建立索引分组的每组的起始时间及组ID。
      * 例如，给定start, end为某天的8:04:33到8:23:15, timeUnit为分钟, every=7, 则切分结果为:
      * [groupId=0]  8:05:00~8:11:59
      * [groupId=1]  8:12:00~8:18:59
      * Aggregation索引会在每个group上分别计算索引值, 并在查询时读取被查询时间区间完全覆盖的group的索引值以加速查询.
      * 返回结果为TreeMap{<8:05:00的时间戳, 0>, <8:12:00的时间戳, 1>}
      * 其中8:05:00是时间轴按分钟划分后, start之后(>=)group开始的最小时间.(通过DateUtils.round函数实现)
      * 8:19:00=8:19:00-0:0:1是时间轴按分钟划分后, end之前的最大时间时间减一秒.
      * 由于every=7, 所以每连续的7分钟作为一组
      * @param start    in second
      * @param end      in second
      * @param every    count
      * @param timeUnit can be Calendar.SECOND|HOUR|DAY|WEEK|SEMI_MONTH|MONTH|YEAR
      * @return TreeMap{Key(group start time), Value(group id)}
      */
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
