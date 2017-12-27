package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.TimeInterval;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;

import java.util.*;

/**
 * Created by song on 17-12-11.
 *
 * similar to Guava's RangeMap, but with custom discrete key: TimePoint.
 */
public class TemporalValue
{
    private final long entityId;
    private final int propertyId;
    private TimePoint validEnd = TimePoint.NOW;

    private NavigableMap<TimePoint, ValueAtTime> values = new TreeMap<>();

    private TemporalValue(int propertyId, long entityId, NavigableMap<TimePoint, ValueAtTime> value)
    {
        this.propertyId = propertyId;
        this.entityId = entityId;
        this.values.put(TimePoint.INIT, ValueAtTime.Invalid);
        this.values.putAll(value);
    }

    public TemporalValue(int propertyId, long entityId)
    {
        this.propertyId = propertyId;
        this.entityId = entityId;
        this.values.put(TimePoint.INIT, ValueAtTime.Invalid);
    }

    public TimeInterval validTimeRange()
    {
        return new TimeInterval(values.ceilingKey(TimePoint.INIT), );
    }

    public boolean contains(TimePoint t)
    {
        return validEnd.compareTo(t)>=0;
    }

    public void put(TimeInterval timeRange, ValueAtTime value)
    {

        TimePoint s = timeRange.getStart();
        TimePoint e = timeRange.getEnd();

        Map.Entry<TimePoint, ValueAtTime> ss = values.floorEntry(s);
        Map.Entry<TimePoint, ValueAtTime> ee = values.floorEntry(e);

        if(ss.getValue().equals(value)){
            // equal value, no need to remove. leave it as it is. merged.
        }else{
            values.put(s, value); // update current or insert a new entry
        }

        if(TimePoint.NOW.equals(e)){
            // do nothing.
        }else{
            values.put(e.post(), ee.getValue());
        }

        NavigableMap<TimePoint, ValueAtTime> sub = values.subMap(s, false, e, true);

        for(TimePoint key : sub.keySet()){
            values.remove(key);
        }
    }

    public ValueAtTime get(TimePoint t)
    {
        return values.floorEntry(t).getValue();
    }

//    public Iterator<TimeValueEntry> iterator()
//    {
//        values.
//        return new Iterator<TimeValueEntry>()
//        {
//            @Override
//            public boolean hasNext()
//            {
//                //return false;
//            }
//
//            @Override
//            public TimeValueEntry next()
//            {
//                //return null;
//            }
//        }
//    }

    public TemporalValue sub(TimeInterval timeRange)
    {
        TimePoint s = timeRange.getStart();
        TimePoint e = timeRange.getEnd();

        Map.Entry<TimePoint, ValueAtTime> ss = values.floorEntry(s);
        Map.Entry<TimePoint, ValueAtTime> ee = values.floorEntry(e);

        NavigableMap<TimePoint, ValueAtTime> data = this.values.subMap(
                timeRange.getStart(), true, timeRange.getEnd(), true);


        if(data.isEmpty() && ss!=null){
            data.put(ss.getKey(), ss.getValue());
        }
        return new TemporalValue(propertyId, entityId, data);
    }
}


//        int h=0, l=0;
//        while(h<high.size() && l<low.size())
//        {
//            TimeValueEntry hItem = high.get(h);
//            TimeValueEntry lItem = low.get(l);
//            TimeInterval ht = hItem.getTime();
//            TimeInterval lt = lItem.getTime();
//            if(cp.compare(ht.getStart(), lt.getStart())<=0)
//            {
//                values.add(hItem);
//                h++;
//            }else{
//                if(cp.compare(lt.getEnd(), ht.getStart())<0){
//                    values.add(lItem);
//                    l++;
//                }else{
//                    TimeValueEntry lItemNew = lItem.clone();
//                    lItemNew.getTime().setEnd(ht.getStart().pre());
//                    values.add(lItemNew);
//                    l++;
//                    lt.setStart(ht.getStart());
//                    values.add(hItem);
//                    h++;
//                }
//            }
//        }
//
//        if(h<high.size())
//        {
//            values.addAll(high.subList(h, high.size()));
//        }
//
//        if(l<low.size())
//        {
//            values.addAll(low.subList(l, low.size()));
//        }
//
//        return values;