/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.act.dynproperty.impl;

import com.google.common.base.Preconditions;


/**
 * |value length|time|ValueType|
 *      30        32     2
 *
 * @author huanghx( huanghx@act.buaa.edu.cn )
 */
public final class SequenceNumber
{
    
    public static final int MAX_VALUE_LENGTH = 1073741823;

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
        return (int)( (num >> 2) & (4294967295L) );
    }
}
