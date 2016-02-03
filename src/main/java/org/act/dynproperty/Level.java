package org.act.dynproperty;

import org.act.dynproperty.impl.FileMetaData;
import org.act.dynproperty.impl.InternalKey;
import org.act.dynproperty.impl.RangeQueryCallBack;
import org.act.dynproperty.impl.ValueType;
import org.act.dynproperty.util.Slice;

public interface Level
{
    public Slice getPointValue( Slice idSlice, int time );
    public Slice getRangeValue( Slice idSlice, int startTime, int endTime, RangeQueryCallBack callback );
    public boolean set( InternalKey key, Slice value );
    public void initfromdisc( FileMetaData metaData );
}
