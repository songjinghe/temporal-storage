package edu.buaa.act.temporal.impl.table;

import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.helper.ByteBufferDataOutput;
import edu.buaa.act.temporal.helper.Slice;

/**
 * Created by song on 2018-01-04.
 */
public class BlockWriter
{
    private final Block block;
    private final ByteBufferDataOutput indexSegment = new ByteBufferDataOutput();

    private int entryCount;
    private int firstEntryPos;
    private long curEID;

    public BlockWriter(Block block){
        this.block = block;
        this.block.clear();

    }

    public int remainingAfterAdd(ETVEntry etvEntry){
//        return block.remaining()-etvEntry.getValue().length()- TimePoint.IO.rawSize();
    }

    public void addETV(ETVEntry etvEntry)
    {
        block.put(etvEntry.getTime().encode()).put(etvEntry.getValue().encode());
        if(curEID!=etvEntry.getEntityId())
        {
            addIndexPos(etvEntry.getEntityId(), block.position());
        }

        curEID = etvEntry.getEntityId();
        entryCount++;
    }

    private void addIndexPos(long entityId, int position)
    {

    }

    public Slice addETVPart(ETVEntry etvEntry)
    {
        block.put(etvEntry.getTime().encode());
    }

    public void finish()
    {

    }

    public boolean isFull()
    {
        return false;
    }

    public void addPreValue(Slice valueToWriteFirst)
    {

    }

    public boolean hasRemaining()
    {
        return false;
    }
}
