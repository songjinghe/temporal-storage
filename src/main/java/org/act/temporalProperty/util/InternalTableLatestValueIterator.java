package org.act.temporalProperty.util;

import java.util.Map.Entry;

import org.act.temporalProperty.impl.InternalKey;

import com.google.common.collect.Maps;

public class InternalTableLatestValueIterator extends AbstractSeekingIterator<InternalKey, Slice> implements InternalIterator
{

    private TableLatestValueIterator tableIterator;
    
    public InternalTableLatestValueIterator( TableLatestValueIterator iterator )
    {
        this.tableIterator = iterator;
    }
    
    @Override
    protected void seekToFirstInternal()
    {
        tableIterator.seekToFirst();
    }

    @Override
    public void seekInternal(InternalKey targetKey)
    {
        tableIterator.seek(targetKey.encode());
    }

    @Override
    protected Entry<InternalKey, Slice> getNextElement()
    {
        if (tableIterator.hasNext()) {
            Entry<Slice, Slice> next = tableIterator.next();
            return Maps.immutableEntry(new InternalKey(next.getKey()), next.getValue());
        }
        return null;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalTableIterator");
        sb.append("{fromIterator=").append(tableIterator);
        sb.append('}');
        return sb.toString();
    }
}
