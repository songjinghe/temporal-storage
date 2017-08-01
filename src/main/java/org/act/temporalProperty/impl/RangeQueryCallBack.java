package org.act.temporalProperty.impl;

import org.act.temporalProperty.util.Slice;
/**
 * 在时间段查询中使用的回调函数
 *
 */
public interface RangeQueryCallBack
{
	void setValueType(String valueType );
    void onCall(int time, Object value);
    void onCallBatch( Slice batchValue );
    Object onReturn();
    enum CallBackType{
    	COUNT,
    	SUM,
    	MIN,
    	MAX,
    	USER;
    }
    CallBackType getType();
}
