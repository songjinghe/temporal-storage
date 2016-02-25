package org.act.dynproperty.util;

import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.act.dynproperty.impl.SeekingIterator;

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
    }

    @Override
    public void seek( Slice targetKey )
    {
    }
    
}
