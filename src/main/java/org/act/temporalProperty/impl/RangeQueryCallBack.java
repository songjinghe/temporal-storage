package org.act.temporalProperty.impl;

import org.act.temporalProperty.util.Slice;
/**
 * 在时间段查询中使用的回调函数
 *
 */
public abstract class RangeQueryCallBack
{
    public enum CallBackType{
        COUNT,
        SUM,
        MIN,
        MAX,
        USER;
    }

	public abstract void setValueType(String valueType);
    public abstract void onCall(int time, Slice value);
    public abstract void onCallBatch( Slice batchValue );
    public abstract Object onReturn();
    public abstract CallBackType getType();
}
