package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.impl.table.ETEntry;
import edu.buaa.act.temporal.impl.iterator.SearchableIterator;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Created by song on 2018-01-02.
 */
public class UnStableFileReader implements PeekingIterator<ETVEntry>, SearchableIterator<ETEntry>
{
    private final ByteBuffer in;
    private final VarLenValueFileUnpackIterator rawToEntry;

    public UnStableFileReader(MappedByteBuffer fileMap)
    {
        this.in = fileMap.duplicate();
        this.in.flip();
        this.in.getLong();//magic num
        this.in.getInt();//file version
        int dataLength = in.getInt();
        int indexLength = in.getInt();
        this.rawToEntry = new VarLenValueFileUnpackIterator(this.in.slice(), dataLength, indexLength);
    }

    @Override
    public void seekToFirst()
    {
        this.rawToEntry.seekToFirst();
    }

    @Override
    public void seekFloor(ETEntry targetKey)
    {
        this.rawToEntry.seekFloor(targetKey);
    }

    @Override
    public ETVEntry peek()
    {
        return this.rawToEntry.peek();
    }

    @Override
    public boolean hasNext()
    {
        return this.rawToEntry.hasNext();
    }

    @Override
    public ETVEntry next()
    {
        return this.rawToEntry.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
