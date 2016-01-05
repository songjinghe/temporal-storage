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

import org.act.dynproperty.util.Slice;
import org.act.dynproperty.util.SliceOutput;
import org.act.dynproperty.util.Slices;

import com.google.common.base.Preconditions;

import static com.google.common.base.Charsets.UTF_8;
import static org.act.dynproperty.util.SizeOf.SIZE_OF_LONG;
import static org.act.dynproperty.util.SizeOf.SIZE_OF_INT;

public class InternalKey
{
    private final Slice Id;
    private final int startTime;
    private final ValueType valueType;

    public InternalKey(Slice Id, int startTime, ValueType valueType)
    {
        Preconditions.checkNotNull(Id, "userKey is null");
        Preconditions.checkArgument(startTime >= 0, "sequenceNumber is negative");
        Preconditions.checkNotNull(valueType, "valueType is null");

        this.Id = Id;
        this.startTime = startTime;
        this.valueType = valueType;
    }

    public InternalKey(Slice data)
    {
        Preconditions.checkNotNull(data, "data is null");
        Preconditions.checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        this.Id = getId( data );
        long packedSequenceAndType = data.getLong( data.length() - SIZE_OF_LONG );
        this.startTime = (int)SequenceNumber.unpackSequenceNumber(packedSequenceAndType);
        this.valueType = SequenceNumber.unpackValueType(packedSequenceAndType);
    }

    public InternalKey(byte[] data)
    {
        this(Slices.wrappedBuffer(data));
    }

    public Slice getId()
    {
        return Id;
    }

    public long getStartTime()
    {
        return (long)startTime;
    }

    public ValueType getValueType()
    {
        return valueType;
    }

    public Slice encode()
    {
        Slice slice = Slices.allocate(Id.length() + SIZE_OF_LONG );
        SliceOutput sliceOutput = slice.output();
        sliceOutput.writeBytes(Id);
        sliceOutput.writeLong(SequenceNumber.packSequenceAndValueType((long)startTime, valueType));
        return slice;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalKey that = (InternalKey) o;

        if (startTime != that.startTime) {
            return false;
        }
        if (Id != null ? !Id.equals(that.Id) : that.Id != null) {
            return false;
        }
        if (valueType != that.valueType) {
            return false;
        }

        return true;
    }

    private int hash;

    @Override
    public int hashCode()
    {
        if (hash == 0) {
            int result = Id != null ? Id.hashCode() : 0;
            result = 31 * result + (int) (startTime ^ (startTime >>> 32));
            result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
            if (result == 0) {
                result = 1;
            }
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        sb.append("{id=").append(getId().getLong( 0 ));
        sb.append( " proId=" ).append( getId().getInt( 8 ) );
        sb.append(", time=").append(getStartTime());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }

    private static Slice getId(Slice data)
    {
        return data.slice(0, data.length() - SIZE_OF_LONG);
    }
}
