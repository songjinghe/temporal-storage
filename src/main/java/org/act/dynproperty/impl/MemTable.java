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
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.act.dynproperty.util.InternalIterator;
import org.act.dynproperty.util.Slice;

import static org.act.dynproperty.util.SizeOf.SIZE_OF_LONG;
import static org.act.dynproperty.util.SizeOf.SIZE_OF_INT;

public class MemTable
        implements SeekingIterable<InternalKey, Slice>
{
    private final ConcurrentSkipListMap<InternalKey, Slice> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();
    
    //Start time of the MemTable
    private int start;

    public MemTable(InternalKeyComparator internalKeyComparator, int startTime )
    {
        table = new ConcurrentSkipListMap<>(internalKeyComparator);
        this.start = startTime;
    }

    public int getStartTime()
    {
        return start;
    }
    
    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemoryUsage()
    {
        return approximateMemoryUsage.get();
    }

    public void add( Slice id,  ValueType valueType, int startTime, Slice value )
    {
        Preconditions.checkNotNull(valueType, "valueType is null");
        Preconditions.checkNotNull(id, "key is null");
        Preconditions.checkNotNull(valueType, "valueType is null");
        
        InternalKey internalKey = new InternalKey(id, startTime, valueType);
        table.put(internalKey, value);

        approximateMemoryUsage.addAndGet( SIZE_OF_LONG + SIZE_OF_INT + SIZE_OF_LONG + value.length());
    }

    public LookupResult get(LookupKey key)
    {
        Preconditions.checkNotNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        //Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        Entry<InternalKey, Slice > entry = table.floorEntry( internalKey );
        if (entry == null) {
            return null;
        }

        InternalKey entryKey = entry.getKey();
        if (entryKey.getId().equals(key.getId())) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            }
            else {
                return LookupResult.ok(key, entry.getValue());
            }
        }
        return null;
    }

    @Override
    public MemTableIterator iterator()
    {
        return new MemTableIterator();
    }

    public class MemTableIterator
            implements InternalIterator
    {
        private PeekingIterator<Entry<InternalKey, Slice>> iterator;

        public MemTableIterator()
        {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst()
        {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seek(InternalKey targetKey)
        {
            InternalKey fromkey = MemTable.this.table.floorKey( targetKey );
            iterator = Iterators.peekingIterator(table.tailMap(fromkey).entrySet().iterator());
        }

        @Override
        public InternalEntry peek()
        {
            Entry<InternalKey, Slice> entry = iterator.peek();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public InternalEntry next()
        {
            Entry<InternalKey, Slice> entry = iterator.next();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

}
