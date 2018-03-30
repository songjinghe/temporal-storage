
package org.act.temporalProperty.impl;

import com.google.common.base.Preconditions;


/**
 * 对InternalKey进行编码的工具类，格式如下
 * |value length|time|ValueType|
 *      30        32     2
 *
 */
public final class SequenceNumber
{
    
    public static final int MAX_VALUE_LENGTH = 0x3fffffff;

    private SequenceNumber()
    {
    }

    public static long packSequenceAndValueType(int time, int valueLength, ValueType valueType)
    {
        Preconditions.checkArgument( valueLength <= MAX_VALUE_LENGTH, "valueLength lager than the max length" );
        Preconditions.checkNotNull(valueType, "valueType is null");

        long toret = 0L;
        toret = toret | valueLength;
        toret = toret << 32;
        toret = toret | time;
        toret = toret << 2;
        toret = toret | valueType.getPersistentId();
        return toret;
    }

    public static ValueType unpackValueType(long num)
    {
        return ValueType.getValueTypeByPersistentId((byte) num);
    }

    public static int unpackValueLength( long num )
    {
        return (int)(num >>> 34);
    }
    
    public static int unpackTime( long num )
    {
        return (int)( (num >> 2) & (0xffffffff) );
    }
}
