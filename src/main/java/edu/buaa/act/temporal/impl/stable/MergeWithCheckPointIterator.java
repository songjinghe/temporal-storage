package edu.buaa.act.temporal.impl.stable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.impl.table.ETEntry;
import edu.buaa.act.temporal.impl.table.ETVEntry;

/**
 * Created by song on 2018-01-03.
 */
public class MergeWithCheckPointIterator extends AbstractIterator<ETVEntry> implements PeekingIterator<ETVEntry>
{
    private final PeekingIterator<ETVEntry> iter;
    private final StableFileReader st;
    private long  curEntityId = -1;

    public MergeWithCheckPointIterator(StableFileReader stReader, PeekingIterator<ETVEntry> iterReader)
    {
        this.st = stReader;
        this.iter = iterReader;
    }

    @Override
    protected ETVEntry computeNext()
    {
        if(iter.hasNext()){
            ETVEntry entry = iter.peek();
            long eid = entry.getEntityId();
            if(curEntityId!=eid){
                curEntityId = eid;
                st.seekFloor(new ETEntry(eid, TimePoint.NOW));
                ETVEntry stEntry = st.peek();
                if(stEntry.getEntityId()==eid)
                {
                    return stEntry;
                }else{ // not contain this eid
                    return iter.next();
                }
            }else{
                return iter.next();
            }
        }else{
            return endOfData();
        }
    }

}
