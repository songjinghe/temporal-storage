package org.act.temporalProperty.table;

import org.act.temporalProperty.impl.InternalKeyComparator;

public final class MemTableComparator extends InternalKeyComparator
{
    private static MemTableComparator instence = null;
    public static synchronized MemTableComparator instence()
    {
        if( instence == null )
            instence = new MemTableComparator();
        return instence;
    }
    private MemTableComparator()
    {
       super( new FixedIdComparator() );
    }
}
