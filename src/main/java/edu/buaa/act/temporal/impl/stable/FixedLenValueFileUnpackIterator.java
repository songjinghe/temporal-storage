package edu.buaa.act.temporal.impl.stable;

import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;
import edu.buaa.act.temporal.impl.iterator.SearchableIterator;
import edu.buaa.act.temporal.impl.table.ETEntry;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static edu.buaa.act.temporal.ValueAtTime.EntityLen;

/**
 * Created by song on 2018-01-05.
 */
public class FixedLenValueFileUnpackIterator implements SearchableIterator<ETEntry, ValueAtTime>
{
    private final ByteBuffer in;
    private final int dataStart;
    private final int indexStart;
    private final int indexLen;
    private int curDataPos;
    private int curIndexPos;

    public FixedLenValueFileUnpackIterator(ByteBuffer fileBuffer, int indexStart, int indexLen, int dataStart)
    {
        this.in = fileBuffer.slice();
        this.in.flip();
        this.indexStart = indexStart;
        this.indexLen = indexLen;
        this.dataStart = dataStart;
        this.curDataPos = dataStart;
        this.curIndexPos = indexStart;
        this.in.position(curDataPos);
    }

    @Override
    public void seekToFirst()
    {
        curIndexPos = indexStart;
        curDataPos = dataStart;
    }

    @Override
    public void seekFloor(ETEntry targetKey)
    {
        curIndexPos = searchFloorETEntry(targetKey);
        int searchEndPos;
        if(hasNextIndex()) {
            searchEndPos = getNextDataEntryPos();
        }else {
            searchEndPos = indexStart;
        }

        curDataPos = in.getInt(curIndexPos+EntityLen+TimePoint.IO.rawSize());
        int prePos = curDataPos;
        while(peek().getKey().compareTo(targetKey)<0 && curDataPos<searchEndPos)
        {
            prePos = curDataPos;
            next();
        }

        if(curDataPos>=searchEndPos || peek().getKey().compareTo(targetKey)>0)
        {
            curDataPos = prePos;
        }
    }

    private int searchFloorETEntry(ETEntry target) // find max eid which is <= entityId, and return its pos in index
    {
        int indexEntryLen = EntityLen +TimePoint.IO.rawSize()+4; // 4 for pos
        int firstIndexPos = indexStart;
        int lastIndexPos = indexStart + indexLen - indexEntryLen;

        for(int intervalLen = (lastIndexPos - firstIndexPos) / indexEntryLen;
            intervalLen > 1;
            intervalLen = (lastIndexPos - firstIndexPos) / indexEntryLen)
        {
            int midIndexPos = (intervalLen / 2) * indexEntryLen;
            int cmp = target.compareTo(in, midIndexPos);
            if (cmp == 0){
                return midIndexPos;
            }else if (cmp < 0){
                lastIndexPos = midIndexPos;
            }else{
                firstIndexPos = midIndexPos;
            }
        }
        int cmp = target.compareTo(in, lastIndexPos);
        if(cmp>0){
            return firstIndexPos;
        }else{
            return lastIndexPos;
        }
    }

    @Override
    public ETVEntry peek()
    {
        if(hasNext())
        {
            long entityId = getCurIndexEID();
            in.mark();
            in.position(curDataPos);
            TimePoint time = TimePoint.IO.decode(in);
            ValueAtTime value = ValueAtTime.decode(in);
            in.reset();
            return new ETVEntry(entityId, time, value);
        }else{
            throw new NoSuchElementException();
        }
    }

    private long getCurIndexEID()
    {
        return in.getLong(indexStart+curIndexPos);
    }

    @Override
    public boolean hasNext()
    {
        return curDataPos<indexStart;
    }

    @Override
    public ETVEntry next()
    {
        if(hasNext())
        {
            long entityId = getCurIndexEID();

            in.position(curDataPos);
            TimePoint time = TimePoint.IO.decode(in);
            ValueAtTime value = ValueAtTime.decode(in);
            curDataPos = in.position();
            if (hasNextIndex())
            {
                int nextDataEntryPos = getNextDataEntryPos();
                if (nextDataEntryPos == curDataPos)
                {
                    incIndexPos();
                }
            }
            return new ETVEntry(entityId, time, value);
        }else{
            throw new NoSuchElementException();
        }
    }

    private void incIndexPos()
    {
        curIndexPos += (EntityLen + TimePoint.IO.rawSize() + 4);
    }

    private boolean hasNextIndex()
    {
        return curIndexPos+EntityLen+TimePoint.IO.rawSize()+4 < indexLen;
    }

    private int getNextDataEntryPos()
    {
        int nextDataPos = indexStart+curIndexPos+
                EntityLen+TimePoint.IO.rawSize()+4+
                EntityLen+TimePoint.IO.rawSize();
        return in.getInt( nextDataPos );
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
