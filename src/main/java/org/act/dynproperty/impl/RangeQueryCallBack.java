package org.act.dynproperty.impl;

import org.act.dynproperty.util.Slice;

public interface RangeQueryCallBack
{
    public void onCall( Slice value );
    public Slice onReturn();
}
