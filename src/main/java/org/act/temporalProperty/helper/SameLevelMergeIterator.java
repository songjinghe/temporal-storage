package org.act.temporalProperty.helper;

import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.table.TwoLevelMergeIterator;

import java.util.Comparator;
import java.util.List;

/**
 * Used in merge process.
 * Note that although disk files' time ranges have no overlap, but their key (proId, entityId, time) range can overlap.
 * So we just use a PriorityQueue to manage this.
 *
 * Created by song on 2018-03-28.
 */
public class SameLevelMergeIterator extends AbstractSearchableIterator
{
    private static Comparator<SearchableIterator> cp = ( o1, o2 ) ->
    {
        if(o1.hasNext() && o2.hasNext()){
            return o1.peek().getKey().compareTo(o2.peek().getKey());
        }else{
            throw new TPSNHException("iterators which ran out should not in heap!");
        }
    };
    private SearchableIterator in;
    private int iteratorCount = 0;

    public SameLevelMergeIterator( List<SearchableIterator> iterators )
    {
        for(SearchableIterator iterator : iterators) add(iterator);
    }

    public SameLevelMergeIterator(){}

    public void add( SearchableIterator append )
    {
        if ( append.hasNext() )
        {
            if ( in == null )
            {
                in = append;
            }
            else if ( cp.compare( in, append ) <= 0 )
            {
                in = TwoLevelMergeIterator.merge( append, in );
            }
            else
            {
                in = TwoLevelMergeIterator.merge( in, append );
            }
            iteratorCount++;
        }
    }

    @Override
    protected InternalEntry computeNext() {
        if ( in != null && in.hasNext() )
        {
            return in.next();
        }else{
            return endOfData();
        }
    }

    public int size()
    {
        return iteratorCount;
    }

    @Override
    public void seekToFirst() {
        super.resetState();
        in.seekToFirst();
    }

    @Override
    public void seek(InternalKey targetKey) {
        super.resetState();
        in.seek( targetKey );
    }
}
