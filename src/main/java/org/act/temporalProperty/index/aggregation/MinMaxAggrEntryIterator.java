package org.act.temporalProperty.index.aggregation;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.index.EntityTimeIntervalEntry;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.TimeIntervalUtil;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;

/**
 * Created by song on 2018-04-06.
 */
public class MinMaxAggrEntryIterator extends AbstractIterator<Triple<Long,Integer,Slice>> implements PeekingIterator<Triple<Long,Integer,Slice>> {
    private final Iterator<EntityTimeIntervalEntry> tpIter;
    private final NavigableSet<Integer> intervalStarts;
    private final int intervalBegin;
    private final int intervalFinish;

    private int eStart;
    private int eEnd;
    private EntityTimeIntervalEntry lastEntry;

    /**
     * @param iterator should only contains one property.
     * @param intervalStarts interval start time point TreeSet
     */
    public MinMaxAggrEntryIterator( Iterator<EntityTimeIntervalEntry> iterator, NavigableSet<Integer> intervalStarts ) {
        this.tpIter = iterator;
        this.intervalStarts = intervalStarts;
        if(intervalStarts.size()<2) throw new TPSNHException("time interval too less!");
        this.intervalBegin = intervalStarts.first();
        this.intervalFinish = intervalStarts.last()-1;
        if(intervalBegin>intervalFinish) throw new TPSNHException("time interval begin > finish!");
    }

    protected Triple<Long,Integer,Slice> computeNext() {
        if(lastEntry!=null){
            return computeTimeGroup(this.eStart, this.eEnd, lastEntry);
        }else {
            while (tpIter.hasNext()) {
                EntityTimeIntervalEntry cur = tpIter.next();
                int eStart = cur.start();
                int eEnd = cur.end();
                if(eEnd<eStart) throw new TPSNHException("end({}) < start({})", eEnd, eStart);
                if(TimeIntervalUtil.overlap(eStart, eEnd, intervalBegin, intervalFinish)){
                    if(eStart<intervalBegin) eStart=intervalBegin;
                    if(eEnd>intervalFinish) eEnd=intervalFinish;
                    return computeTimeGroup(eStart, eEnd, cur);
                }//else: not in [intervalBegin, intervalFinish], do nothing, continue next loop;
            }
            return endOfData();
        }
    }

    private Triple<Long,Integer,Slice> computeTimeGroup(int eStart, int eEnd, EntityTimeIntervalEntry entry) {
        int timeGroupId = intervalStarts.floor(eStart);
        // if eStart and eEnd both in the same time range.
        if(Objects.equals(timeGroupId, intervalStarts.floor(eEnd))){
            lastEntry = null;
            return Triple.of(entry.entityId(), timeGroupId, entry.value());
        }else{
            this.eStart = intervalStarts.higher(eStart);
            this.eEnd = eEnd;
            lastEntry = entry;
            return Triple.of(entry.entityId(), timeGroupId, entry.value());
        }
    }

}
