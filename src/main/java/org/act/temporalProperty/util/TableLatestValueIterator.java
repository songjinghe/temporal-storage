package org.act.temporalProperty.util;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.table.BlockEntry;

/**
 * 在生成新的StableFile的时候，需要把上一个tableFile的每个动态属性的最新的值加入到新的文件中，这个类就是提取StableFile中每个动态属性的最近值的工具
 */
public class TableLatestValueIterator implements SeekingIterator<Slice, Slice>
{
    
    private SeekingIterator<Slice,Slice> iterator;
    private Entry<Slice,Slice> next = null;
    private Entry<Slice,Slice> next_next = null; 
    
    public TableLatestValueIterator(SeekingIterator<Slice,Slice> iterator)
    {
        this.iterator = iterator;
    }

    @Override
    public Entry<Slice,Slice> peek()
    {
        if( hasNext() )
            return next;
        else
            throw new NoSuchElementException();
    }

    @Override
    public Entry<Slice,Slice> next()
    {
        if( hasNext() )
        {
            Entry<Slice,Slice> toret = this.next;
            this.next = null;
            return toret;
        }
        else
            throw new NoSuchElementException();
    }

    private void getnext()
    {
        if( this.next_next == null )
        {
            try
            {
                this.next = this.iterator.next();
                this.next_next = this.iterator.next();
                while( this.next.getKey().copySlice( 0, 12 ).equals( this.next_next.getKey().copySlice( 0, 12 ) ) )
                {
                    this.next = this.next_next;
                    this.next_next = this.iterator.next();
                }
            }
            catch( NoSuchElementException e )
            {}
        }
        else
        {
            this.next = this.next_next;
            try
            {
                this.next_next = this.iterator.next();
                while( this.next.getKey().copySlice( 0, 12 ).equals( this.next_next.getKey().copySlice( 0, 12 ) ) )
                {
                    this.next = this.next_next;
                    this.next_next = this.iterator.next();
                }
            }
            catch( NoSuchElementException e )
            {
                this.next_next = null;
            }
        }
    }
    
    @Override
    public void remove()
    {
        next();
    }

    @Override
    public boolean hasNext()
    {
        if( null != next )
            return true;
        else
        {
            getnext();
            return (null != next);
        }
            
    }

    @Override
    public void seekToFirst()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek( Slice targetKey )
    {
        throw new UnsupportedOperationException();
    }

    private static class ChangeTimeIterator extends AbstractIterator<Entry<Slice,Slice>> implements SeekingIterator<Slice,Slice>{
        private final SeekingIterator<Slice, Slice> input;
        private final int startTime;

        /**
         * update every entry key's startTime to `startTime`.
         * @param input
         * @param startTime
         */
        ChangeTimeIterator(SeekingIterator<Slice,Slice> input, int startTime){
            this.input = input;
            this.startTime = startTime;
        }

        @Override
        protected Entry<Slice, Slice> computeNext() {
            if(input.hasNext()){
                Entry<Slice, Slice> entry = input.next();
                InternalKey key = new InternalKey(entry.getKey());
                InternalKey newKey = new InternalKey(key.getId(), startTime, key.getValueLength(), key.getValueType());
                return new BlockEntry(newKey.encode(), entry.getValue());
            }else{
                return endOfData();
            }
        }

        @Override
        public void seekToFirst() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seek(Slice targetKey) {
            throw new UnsupportedOperationException();
        }
    }

    public static SeekingIterator<Slice,Slice> setNewStart(SeekingIterator<Slice,Slice> input, int time){
        return new ChangeTimeIterator(new TableLatestValueIterator(input), time);
    }
}
