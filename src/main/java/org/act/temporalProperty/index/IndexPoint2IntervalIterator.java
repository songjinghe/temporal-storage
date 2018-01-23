package org.act.temporalProperty.index;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by song on 2018-01-19.
 */
public class IndexPoint2IntervalIterator extends AbstractIterator<IndexIntervalEntry> implements PeekingIterator<IndexIntervalEntry>{
    private final PeekingIterator<IndexPointEntry> tpIter;
    private final int endTime;

    public IndexPoint2IntervalIterator(Iterator<IndexPointEntry> in, int endTime){
        this.tpIter = Iterators.peekingIterator(in);
        this.endTime = endTime;
    }

    protected IndexIntervalEntry computeNext()
    {
        if(tpIter.hasNext()){
            IndexPointEntry tpv1 = tpIter.next();
            if(tpIter.hasNext()){
                IndexPointEntry tpv2 = tpIter.peek();
                if(tpv2.getEntityId()==tpv1.getEntityId()){
                    return new IndexIntervalEntry(tpv1.getEntityId(), tpv1.getTimePoint(), tpv2.getTimePoint()-1, tpv1.getValue());
                }else{
                    return new IndexIntervalEntry(tpv1.getEntityId(), tpv1.getTimePoint(), endTime, tpv1.getValue());
                }
            }else{
                return new IndexIntervalEntry(tpv1.getEntityId(), tpv1.getTimePoint(), endTime, tpv1.getValue());
            }
        }else{
            return endOfData();
        }
    }
}
