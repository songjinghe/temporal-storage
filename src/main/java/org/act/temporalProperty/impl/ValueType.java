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
package org.act.temporalProperty.impl;

public enum ValueType
{
    VALUE( 0 ),
    INVALID( 1 ),
    /**
     * means this entry is inserted by system when user performs a time interval modification operation.
     * for example, if we have temporal property <9, A>, <20, B> (9 and 20 is start time point, A and B is value) and
     * perform a time interval modification:
     * setProperty(12, 16, C) (12 is start, 16 is end, C is value)
     * then we got:
     * <9, A>,  <12, C>, <17(UNKNOWN), ?>, <20, B>
     * so 17 is marked UNKNOWN, and its value is either:
     * - queried by system before apply the time interval operation. or
     * - left with any random value which should not be relied on, should query low-level disk layer for correct value.
     * note that 12 is **NOT** marked UNKNOWN.
     */
    UNKNOWN( 2 );

    public static ValueType getValueTypeByPersistentId( int persistentId )
    {
        persistentId = persistentId & 3;
        switch ( persistentId )
        {
        case 0:
            return VALUE;
        case 1:
            return INVALID;
        case 2:
            return UNKNOWN;
        default:
            throw new IllegalArgumentException( "invalid persistentId!" );
        }
    }

    private final int persistentId;

    ValueType( int persistentId )
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }
}
