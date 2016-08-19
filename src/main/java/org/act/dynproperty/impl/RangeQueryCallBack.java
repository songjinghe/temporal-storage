package org.act.dynproperty.impl;

import org.act.dynproperty.util.Slice;
/**
 * 在时间段查询中使用的回调函数
 *
 */
public interface RangeQueryCallBack
{
	public void setValueType(String valueType );
    public void onCall( Slice value );
    public Slice onReturn();
}
