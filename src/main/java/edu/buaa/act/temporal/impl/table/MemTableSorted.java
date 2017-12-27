package edu.buaa.act.temporal.impl.table;

import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.impl.TimePointValueEntry;

import java.util.*;

/**
 * Created by song on 17-12-11.
 */
public class MemTableSorted
{
    private TreeMap<PETKey, ValueAtTime> data = new TreeMap<>();
    private long currentSize;


    public ValueAtTime getValueAtTimePoint(int propertyId, long entityId, TimePoint time)
    {
        PETKey searchKey = new PETKey(propertyId, entityId, time);
        Map.Entry<PETKey, ValueAtTime> s = data.floorEntry(searchKey);
        if(s!=null){
            PETKey got = s.getKey();
            if(got.getPropertyId()==propertyId && got.getEntityId()==entityId)
            {
                return s.getValue();
            }else{
                return null;
            }
        }else{
            return null;
        }
    }

    public List<TimeValueEntry> getTimeValues(int propertyId, long entityId, TimePoint start, TimeValueEntryCallback end)
    {
        PETKey s = new PETKey(propertyId, entityId, start);
        PETKey e = new PETKey(propertyId, entityId, end);

        NavigableMap<PETKey, ValueAtTime> subMap = data.subMap(s, true, e, true);

        Map.Entry<PETKey, ValueAtTime> pre = data.floorEntry(s);
        Map.Entry<PETKey, ValueAtTime> post = data.ceilingEntry(e);

        List<TimePointValueEntry> result = new ArrayList<>(subMap.size()+1);

        if(pre!=null && !pre.getKey().equals(s))
        {
            PETKey got = pre.getKey();
            if(got.getPropertyId()==propertyId && got.getEntityId()==entityId)
            {
                result.add(new TimePointValueEntry(pre.getKey().getTime(), pre.getValue()));
            }
        }

        for(Map.Entry<PETKey, ValueAtTime> entry : subMap.entrySet())
        {
            result.add(new TimePointValueEntry(entry.getKey().getTime(), entry.getValue()));
        }

        if(post!=null && !post.getKey().equals(e))
        {
            PETKey got = post.getKey();
            if(got.getPropertyId()==propertyId && got.getEntityId()==entityId)
            {
                return toTimeInterval(result, post.getKey().getTime(), false);
            }
        }
        return toTimeInterval(result, TimePoint.NOW, true);
    }

    private List<TimeValueEntry> toTimeInterval(List<TimePointValueEntry> in, TimePoint end, boolean endInclusive)
    {
        List<TimeValueEntry> result = new ArrayList<>();
        for(int i=1; i<in.size(); i++)
        {
            TimePointValueEntry pre = in.get(i - 1);
            TimePointValueEntry cur = in.get(i);
            result.add(new TimeValueEntry(new TimeInterval(pre.getTime(), cur.getTime().pre()), pre.getValue()));
        }
        TimePointValueEntry last = in.get(in.size() - 1);
        TimePoint realEnd;
        if(endInclusive)
        {
            realEnd = end;
        }else{
            realEnd = end.post();
        }
        result.add(new TimeValueEntry(new TimeInterval(last.getTime(), realEnd), last.getValue()));
        return result;
    }
}
