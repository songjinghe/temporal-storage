package edu.buaa.act.temporal.helper;

import edu.buaa.act.temporal.TimeInterval;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.TimeValueEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by song on 17-12-8.
 */
public class DataEntryUtils
{
    // data in high will shadow data in low if there are conflict.
    public static List<TimeValueEntry> mergeList(List<TimeValueEntry> high, List<TimeValueEntry> low)
    {
        List<TimeValueEntry> result = new ArrayList<TimeValueEntry>(high.size() + low.size());
        Comparator<TimePoint> cp = TimePoint.ComparatorASC;

        int h=0, l=0;
        while(h<high.size() && l<low.size())
        {
            TimeValueEntry hItem = high.get(h);
            TimeValueEntry lItem = low.get(l);
            TimeInterval ht = hItem.getTime();
            TimeInterval lt = lItem.getTime();
            if(cp.compare(ht.getStart(), lt.getStart())<=0)
            {
                result.add(hItem);
                h++;
            }else{
                if(cp.compare(lt.getEnd(), ht.getStart())<0){
                    result.add(lItem);
                    l++;
                }else{
                    TimeValueEntry lItemNew = lItem.copy();
                    lItemNew.getTime().setEnd(ht.getStart().pre());
                    result.add(lItemNew);
                    l++;
                    lt.setStart(ht.getStart());
                    result.add(hItem);
                    h++;
                }
            }
        }

        if(h<high.size())
        {
            result.addAll(high.subList(h, high.size()));
        }

        if(l<low.size())
        {
            result.addAll(low.subList(l, low.size()));
        }

        return result;
    }

}
