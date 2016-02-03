package org.act.dynproperty;

import org.act.dynproperty.impl.RangeQueryCallBack;
import org.act.dynproperty.impl.ValueType;
import org.act.dynproperty.util.Slice;

public interface DynPropertyStore
{
    public Slice getPointValue( long id, int proId, int time );
    
    public Slice getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback );
    
    public boolean setProperty( Slice id, byte[] value );
    
    public boolean delete(Slice id);
}
