
package org.act.temporalProperty.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.act.temporalProperty.table.UserComparator;
import org.act.temporalProperty.util.Slice;
/**
 * MemTable结构，是动态属性在内存中的存储结构
 *
 */
public class MemTable
        implements SeekingIterable<Slice, Slice>
{
	/**
	 * 所有的数据存储在table中，使用SkipListMap
	 */
    private final ConcurrentSkipListMap<Slice, MemEntry> table;
    /**
     * 所占用的空间
     */
    private final AtomicLong approximateMemoryUsage = new AtomicLong();
    
    /**
     * 当前MemTable中数据的最小有效时间
     */
    private int start = Integer.MAX_VALUE;
    
    /**
     * 当前MemTable中数据的最大有效时间
     */
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

    /**
     * 向MemTable中增加动态属性数据
     * @param key 动态属性record经过InternalKey编码后的key
     * @param value 值
     */
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

    /**
     * 从MemTable中查询相应数据
     * @param key 动态属性record经过InternalKey编码后的key
     */
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

    /**
     * 返回能够对整个MemTable提供便利的iterator
     */
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
    
    /**
     * 对MemTable提供遍历功能的Iterator
     *
     */
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

        /**
         * 将指针指向第一个数据项
         */
        @Override
        public void seekToFirst()
        {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        /**
         * 将指针指向制定key的数据项
         */
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
