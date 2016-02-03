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

import org.act.dynproperty.table.UserComparator;
import org.act.dynproperty.util.Slice;

public class MemTable
        implements SeekingIterable<Slice, Slice>
{
    private final ConcurrentSkipListMap<Slice, MemEntry> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();
    
    //Start time of the MemTable
    private int start = Integer.MAX_VALUE;
    private int end = -1;

    public MemTable(UserComparator internalKeyComparator )
    {
        table = new ConcurrentSkipListMap<>(internalKeyComparator);
    }

    public int getStartTime()
    {
        return start;
    }
    
    public int getEndTime()
    {
        return end;
    }
    
    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemoryUsage()
    {
        return approximateMemoryUsage.get();
    }

    public void add( Slice key, Slice value )
    {
        Preconditions.checkArgument( key.length() == 20, "key should all be 20 bytes" );
        InternalKey internalKey = new InternalKey( key );
        int startTime = internalKey.getStartTime();
        if( startTime < this.start )
            this.start = startTime;
        if( startTime > this.end )
            this.end = startTime;
        table.put(key, new MemEntry( key, value ));

        approximateMemoryUsage.addAndGet( key.length() + value.length());
    }

    public Slice get(Slice key)
    {
        Preconditions.checkNotNull(key, "key is null");

        //Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        Entry<Slice, MemEntry > entry = table.floorEntry( key );
        if (entry == null) {
            return null;
        }

        Slice entryKey = entry.getValue().getKey();
        InternalKey ansKey = new InternalKey( entryKey );
        InternalKey lookKey = new InternalKey( key );
        if( !ansKey.getId().equals( lookKey.getId() ) )
            return null;
        return entry.getValue().getValue().copySlice( 0, ansKey.getValueLength() );
    }

    @Override
    public MemTableIterator iterator()
    {
        return new MemTableIterator();
    }

    
    private class MemEntry implements Entry<Slice,Slice>
    {

        private Slice key;
        private Slice value;
        
        MemEntry(Slice key, Slice value)
        {
            this.key = key;
            this.value = value;
        }
        
        @Override
        public Slice getKey()
        {
            return key;
        }

        @Override
        public Slice getValue()
        {
            return value;
        }

        @Override
        public Slice setValue( Slice value )
        {
            Slice former = this.value;
            this.value = value;
            return former;
        }
        
    }
    
    public class MemTableIterator
            implements SeekingIterator<Slice,Slice>
    {
        private PeekingIterator<Entry<Slice, MemEntry>> iterator;

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
        public void seek(Slice targetKey)
        {
            Slice fromkey = MemTable.this.table.floorKey( targetKey );
            if( null == fromkey )
            {
                iterator = Iterators.peekingIterator(table.entrySet().iterator());
            }
            else    
                iterator = Iterators.peekingIterator(table.tailMap(fromkey).entrySet().iterator());
        }

        @Override
        public Entry<Slice, Slice> peek()
        {
            Entry<Slice, MemEntry> entry = iterator.peek();
            return entry.getValue();
        }

        @Override
        public Entry<Slice, Slice> next()
        {
            Entry<Slice, MemEntry> entry = iterator.next();
            return entry.getValue();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

}
