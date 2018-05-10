package org.act.temporalProperty.impl;

import com.google.common.collect.AbstractIterator;

/**
 * Created by song on 2018-05-09.
 */
public class UnknownToInvalidIterator extends AbstractIterator<InternalEntry> implements SearchableIterator
{

    private final SearchableIterator in;

    public UnknownToInvalidIterator( SearchableIterator in ) {this.in = in;}

    @Override
    protected InternalEntry computeNext()
    {
        if ( in.hasNext() )
        {
            InternalEntry entry = in.next();
            InternalKey key = entry.getKey();
            if ( key.getValueType() == ValueType.UNKNOWN )
            {
                return new InternalEntry( new InternalKey( key.getId(), key.getStartTime(), ValueType.INVALID ), entry.getValue() );
            }
            else
            {
                return entry;
            }
        }
        else
        { return endOfData(); }
    }

    @Override
    public void seekToFirst()
    {
        in.seekToFirst();
    }

    @Override
    public void seek( InternalKey targetKey )
    {
        in.seek( targetKey );
    }
}
