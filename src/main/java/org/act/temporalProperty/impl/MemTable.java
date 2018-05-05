package org.act.temporalProperty.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.exception.ValueUnknownException;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.table.FixedIdComparator;
import org.act.temporalProperty.table.UserComparator;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.Slices;

/**
 * Modified MemTable, which stores time interval only.
 */
public class MemTable
{
    private static final long NOW = Long.MAX_VALUE;
    private final Map<Slice,TreeMap<TimeIntervalKey,Slice>> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();
    private final Comparator<TimeIntervalKey> cp;

    public MemTable( UserComparator internalKeyComparator )
    {
        this();
    }

    public MemTable()
    {
        this.cp = Comparator.comparingLong( TimeIntervalKey::getStart );
        this.table = new TreeMap<>( new FixedIdComparator() );
    }

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemUsage()
    {
        return approximateMemoryUsage.get();
    }

    public void addToNow( InternalKey key, Slice value )
    {
        Preconditions.checkArgument( key.getValueType() != ValueType.UNKNOWN );
        Slice id = key.getId();
        if ( !table.containsKey( id ) )
        {
            table.put( id, new TreeMap<>( cp ) );
        }
        addEntry( table.get( id ), new TimeIntervalKey( key, NOW ), value );
    }

    public void addInterval( InternalKey key, int endTime, Slice value )
    {
        Preconditions.checkArgument( key.getValueType() != ValueType.UNKNOWN );
        Preconditions.checkArgument( key.getStartTime() <= endTime );
        Slice id = key.getId();
        if ( !table.containsKey( id ) )
        {
            table.put( id, new TreeMap<>( cp ) );
        }
        addEntry( table.get( id ), new TimeIntervalKey( key, endTime ), value );
    }

    public void addInterval( TimeIntervalKey key, Slice value )
    {
        Preconditions.checkNotNull( key );
        InternalKey startKey = key.getStartKey();
        Preconditions.checkArgument( startKey.getValueType() != ValueType.UNKNOWN );
        Preconditions.checkArgument( startKey.getStartTime() <= key.getEnd() );
        Slice id = startKey.getId();
        if ( !table.containsKey( id ) )
        {
            table.put( id, new TreeMap<>( cp ) );
        }
        addEntry( table.get( id ), key, value );
    }

    private void addEntry( TreeMap<TimeIntervalKey,Slice> entityMap, TimeIntervalKey key, Slice value )
    {
        Entry<TimeIntervalKey,Slice> smaller = entityMap.lowerEntry( key );
        if ( smaller != null && smaller.getKey().getEnd() >= key.getStart() )
        {
            TimeIntervalKey smallKey = smaller.getKey();
            TimeIntervalKey adjustedKey = smallKey.changeEnd( key.getStart() - 1 );
            entityMap.remove( smallKey );
            entityMap.put( adjustedKey, smaller.getValue() );
            if ( smallKey.getEnd() > key.getEnd() ) //split an exist interval into two, no need to check post entries.
            {
                TimeIntervalKey tail = smallKey.changeStart( key.getEnd() + 1 );
                entityMap.put( tail, smaller.getValue() );
                entityMap.put( key, value );
                return;
            }
        }

        Iterator<Entry<TimeIntervalKey,Slice>> iter = entityMap.tailMap( key, true ).entrySet().iterator();
        while ( iter.hasNext() )
        {
            Entry<TimeIntervalKey,Slice> entry = iter.next();
            TimeIntervalKey tmp = entry.getKey();
            if ( tmp.getEnd() <= key.getEnd() )
            {
                iter.remove();
            }
            else if ( tmp.getStart() <= key.getEnd() )
            {
                TimeIntervalKey adjusted = tmp.changeStart( key.getEnd() + 1 );
                iter.remove();
                entityMap.put( adjusted, entry.getValue() );
                break;
            }
        }
        entityMap.put( key, value );
        approximateMemoryUsage.addAndGet( value.length() + 20 ); // 20 is key.length()
    }

    public Slice get( InternalKey key ) throws ValueUnknownException
    {
        Preconditions.checkNotNull( key, "key is null" );
        TreeMap<TimeIntervalKey,Slice> entityMap = table.get( key.getId() );
        if ( entityMap == null )
        {
            throw new ValueUnknownException(); //no entity
        }
        TimeIntervalKey tmpKey = new TimeIntervalKey( key, NOW );
        Entry<TimeIntervalKey,Slice> entry = entityMap.floorEntry( tmpKey );
        if ( entry != null )
        {
            TimeIntervalKey ansKey = entry.getKey();
            if ( ansKey.getEnd() >= key.getStartTime() )
            {
                if ( ansKey.getKey().getValueType() != ValueType.INVALID )
                {
                    return entry.getValue();
                }
                else //else: invalid value
                {
                    return null;
                }
            }
            else //else: no value in this interval
            {
                throw new ValueUnknownException();
            }
        }
        else //else: earlier than smallest time
        {
            throw new ValueUnknownException();
        }
    }

    public void merge( MemTable toMerge )
    {
        this.table.putAll( toMerge.table );
    }

    public static Slice encode(TimeIntervalKey key, Slice value)
    {
        DynamicSliceOutput out = new DynamicSliceOutput( 64 );
        out.writeLong( key.getEnd() );
        Slice start = key.getStartKey().encode();
        out.writeInt( start.length() );
        out.writeBytes( start );
        out.writeInt( value.length() );
        out.writeBytes( value );
        return out.slice();
    }

    public static TimeIntervalValueEntry decode(SliceInput in)
    {
        long endTime = in.readLong();
        int len = in.readInt();
        InternalKey start = new InternalKey( in.readSlice( len ) );
        len = in.readInt();
        Slice value = in.readSlice( len );
        return new TimeIntervalValueEntry( new TimeIntervalKey( start, endTime ), value );
    }

    public MemTableIterator iterator()
    {
        return new MemTableIterator(table);
    }

    public PeekingIterator<Entry<TimeIntervalKey,Slice>> intervalEntryIterator()
    {
        return new IntervalIterator();
    }

    public static class MemTableIterator extends AbstractIterator<InternalEntry> implements SearchableIterator
    {
        private final Map<Slice,TreeMap<TimeIntervalKey,Slice>> table;
        private PeekingIterator<Entry<Slice,TreeMap<TimeIntervalKey,Slice>>> iterator;
        private PeekingIterator<Entry<TimeIntervalKey,Slice>> entryIter;
        private boolean nextIsStart = true;
        private boolean allowSeek = true;
        private boolean seekHasNext = false;

        public MemTableIterator(Map<Slice,TreeMap<TimeIntervalKey,Slice>> table)
        {
            this.table = table;
            this.iterator = Iterators.peekingIterator( table.entrySet().iterator() );
        }

        @Override
        protected InternalEntry computeNext()
        {
            allowSeek = false;
            if ( !seekHasNext )
            {
                return endOfData();
            }

            while ( entryIter == null || !entryIter.hasNext() )
            {
                if ( iterator.hasNext() )
                {
                    nextIsStart = true;
                    entryIter = Iterators.peekingIterator( iterator.next().getValue().entrySet().iterator() );
                }
                else
                {
                    return endOfData();
                }
            }

            while ( entryIter.hasNext() )
            {
                Entry<TimeIntervalKey,Slice> entry = entryIter.peek();
                InternalKey key = entry.getKey().getStartKey();
                if ( nextIsStart )
                {
                    nextIsStart = false;
                    return new InternalEntry( key, entry.getValue() );
                }
                else
                {
                    entryIter.next();
                    if ( entryIter.hasNext() )
                    {
                        Entry<TimeIntervalKey,Slice> nextEntry = entryIter.peek();
                        if ( nextEntry.getKey().getStartKey().getStartTime() == key.getStartTime() + 1 )
                        {
                            nextIsStart = true;
                            //continue;
                        }
                        else
                        {
                            nextIsStart = true;
                            return new InternalEntry( entry.getKey().getEndKey(), Slices.EMPTY_SLICE );
                        }
                    }
                    else
                    {
                        nextIsStart = true;
                        return new InternalEntry( entry.getKey().getEndKey(), Slices.EMPTY_SLICE );
                    }
                }
            }
            throw new TPSNHException( "should never reach here!" );
        }

        @Override
        public void seekToFirst()
        {
            if ( !allowSeek )
            {
                throw new TPSNHException( "should call seek before call hasNext" );
            }
            iterator = Iterators.peekingIterator( table.entrySet().iterator() );
            entryIter = null;
        }

        @Override
        public void seek( InternalKey targetKey )
        {
            if ( !allowSeek )
            {
                throw new TPSNHException( "should call seek before call hasNext" );
            }
            TreeMap<TimeIntervalKey,Slice> entityMap = table.get( targetKey.getId() );
            if ( entityMap == null )
            {
                seekHasNext = false;
                return;
            }
            TimeIntervalKey searchKey = new TimeIntervalKey( targetKey, NOW );
            TimeIntervalKey higher = entityMap.ceilingKey( searchKey );
            if ( null == higher )
            {
                seekHasNext = false;
            }
            else
            {
                seekHasNext = true;
                entryIter = Iterators.peekingIterator( entityMap.tailMap( searchKey, true ).entrySet().iterator() );
            }
        }
    }

    private class IntervalIterator extends AbstractIterator<Entry<TimeIntervalKey,Slice>> implements PeekingIterator<Entry<TimeIntervalKey,Slice>>
    {

        private PeekingIterator<Entry<Slice,TreeMap<TimeIntervalKey,Slice>>> iterator;
        private PeekingIterator<Entry<TimeIntervalKey,Slice>> entryIter;

        private IntervalIterator()
        {
            iterator = Iterators.peekingIterator( table.entrySet().iterator() );
        }

        @Override
        protected Entry<TimeIntervalKey,Slice> computeNext()
        {
            while ( entryIter == null || !entryIter.hasNext() )
            {
                if ( iterator.hasNext() )
                {
                    entryIter = Iterators.peekingIterator( iterator.next().getValue().entrySet().iterator() );
                }
                else
                {
                    return endOfData();
                }
            }
            return entryIter.next();
        }
    }

    public static class TimeIntervalValueEntry implements Entry<TimeIntervalKey, Slice>{

        private final TimeIntervalKey key;
        private final Slice val;

        private TimeIntervalValueEntry(TimeIntervalKey key, Slice val){
            this.key = key;
            this.val = val;
        }
        @Override
        public TimeIntervalKey getKey()
        {
            return key;
        }

        @Override
        public Slice getValue()
        {
            return val;
        }

        @Override
        public Slice setValue( Slice value )
        {
            throw new UnsupportedOperationException();
        }
    }
}
