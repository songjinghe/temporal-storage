package org.act.temporalProperty.table;

import java.util.Map.Entry;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.impl.MemTable.MemTableIterator;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.Slice;

/**
 * 将某个文件及其Buffer中的内容合并，并组成同意的Iterator。在查询，文件合并的过程中起到作用：包括排除有delete标记的record、从文件中和Buffer中返回正确的值
 *
 */
public class BufferFileAndTableIterator implements SeekingIterator<Slice,Slice>
{

    private SeekingIterator<Slice,Slice> memIterator;
    private SeekingIterator<Slice,Slice> tableIterator;
    private Entry<Slice,Slice> next;
    private Comparator<Slice> comparator;
    
    public BufferFileAndTableIterator( SeekingIterator<Slice,Slice> latest, SeekingIterator<Slice,Slice> old, Comparator<Slice> comparator )
    {
        this.memIterator = latest;
        this.tableIterator = old;
        this.next = null;
        this.comparator = comparator;
    }
    
    @Override
    public boolean hasNext()
    {
        if( null != next )
            return true;
        else
            getNextElement();
        return next != null;
    }

    private void getNextElement()
    {
        if( null != this.memIterator && null != this.tableIterator )
        {
            Entry<Slice,Slice> memEntry = null;
            Entry<Slice,Slice> tableEntry = null;
            try
            {
                memEntry = this.memIterator.peek();
            }
            catch( NoSuchElementException e )
            {
                this.memIterator = null;
            }
            try
            {
                tableEntry = this.tableIterator.peek();
            }
            catch( NoSuchElementException e )
            {
                this.tableIterator = null;
            }
            if( memEntry == null || tableEntry == null )
            {
                getNextElement();
                return;
            }
            if( comparator.compare( memEntry.getKey(), tableEntry.getKey() ) == 0 )
            {
                InternalKey key = new InternalKey(memEntry.getKey());
                if( key.getValueType().getPersistentId() == ValueType.DELETION.getPersistentId() )
                {
                    this.memIterator.next();
                    this.tableIterator.next();
                    getNextElement();
                    return;
                }
                next = memEntry;
                this.memIterator.next();
                this.tableIterator.next();
                
            }
            else if( comparator.compare( memEntry.getKey(), tableEntry.getKey() ) > 0 )
            {
                next = tableEntry;
                this.tableIterator.next();
            }
            else
            {
                next = memEntry;
                this.memIterator.next();
            }
        }
        else if( null == this.memIterator && null != this.tableIterator )
        {
            Entry<Slice,Slice> tableEntry = null;
            try
            {
                tableEntry = this.tableIterator.peek();
            }
            catch( NoSuchElementException e )
            {
                this.tableIterator = null;
            }
            if( null == tableEntry )
            {
                getNextElement();
                return;
            }
            next = tableEntry;
            this.tableIterator.next();
        }
        else if( null != this.memIterator && null == this.tableIterator )
        {
            Entry<Slice,Slice> memEntry = null;
            try
            {
                memEntry = this.memIterator.peek();
            }
            catch( NoSuchElementException e )
            {
                this.memIterator = null;
            }
            if( null == memEntry )
            {
                getNextElement();
                return;
            }
            InternalKey key = new InternalKey(memEntry.getKey());
            if( key.getValueType().getPersistentId() == ValueType.DELETION.getPersistentId() )
            {
                getNextElement();
                return;
            }
            next = memEntry;
            this.memIterator.next();
        }
        else
        {
            next = null;
        }
    }

    @Override
    public Entry<Slice,Slice> next()
    {
        Entry<Slice,Slice> toret;
        if( hasNext() )
        {
            toret = next;
            next = null;
            return toret;
        }
        else
            throw new NoSuchElementException();
    }

    @Override
    public Entry<Slice,Slice> peek()
    {
        if( null == next )
            getNextElement();
        if( null == next )
            throw new NoSuchElementException();
        return next;
    }

    @Override
    public void seekToFirst()
    {
        this.memIterator.seekToFirst();
        this.tableIterator.seekToFirst();
    }

    @Override
    public void seek( Slice targetKey )
    {
        this.memIterator.seek( targetKey );
        this.tableIterator.seek( targetKey );
    }

    @Override
    public void remove()
    {
        next();
    }
}
