package org.act.dynproperty;

import org.act.dynproperty.impl.DynPropertyStoreImpl;

public class DynPropertyStoreFactory
{
    public static DynPropertyStore newPropertyStore( String dbDir )
    {
        return new DynPropertyStoreImpl( dbDir );
    }
}
