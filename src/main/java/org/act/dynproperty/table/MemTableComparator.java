package org.act.dynproperty.table;

import org.act.dynproperty.impl.InternalKeyComparator;
import org.act.dynproperty.table.BytewiseComparator;

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
       super( new BytewiseComparator() );
    }
}
