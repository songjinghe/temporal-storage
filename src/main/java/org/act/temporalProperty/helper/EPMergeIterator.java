package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.table.TwoLevelMergeIterator;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * Created by song on 2018-01-24.
 */
public class EPMergeIterator extends TwoLevelMergeIterator
{
    private final Slice id;

    public EPMergeIterator(Slice idSlice, SearchableIterator old, SearchableIterator latest) {
        super( isEP( latest ) ? latest : new EPEntryIterator( idSlice, latest ), isEP( old ) ? old : new EPEntryIterator( idSlice, old ) );
        this.id = idSlice;
    }

    private static boolean isEP( SearchableIterator iterator )
    {
        return  (iterator instanceof EPEntryIterator) ||
                (iterator instanceof EPAppendIterator) ||
                (iterator instanceof EPMergeIterator);
    }

    @Override
    protected InternalEntry computeNext() {
        InternalEntry entry = super.computeNext();
        if ( entry != null )
        {
            if ( entry.getKey().getId().equals( id ) )
            {
                return entry;
            }
            else
            {
                throw new TPSNHException( "id not equal!" );
            }
        }
        else
        { return endOfData(); }
    }
}
