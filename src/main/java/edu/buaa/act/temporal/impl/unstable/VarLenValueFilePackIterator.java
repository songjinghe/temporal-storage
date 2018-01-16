package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.helper.ByteBufferDataOutput;
import edu.buaa.act.temporal.helper.Slice;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by song on 2018-01-04.
 */
public class VarLenValueFilePackIterator extends AbstractIterator<Slice> implements PeekingIterator<Slice>
{
    private final int blockSize;
    private final int approximateSegmentSize; // returned byte[] 's length always <= blockSize
    private final PeekingIterator<ETVEntry> iter;
    private final ByteBufferDataOutput indexSegment = new ByteBufferDataOutput();
    private MessageDigest md;

    private int startPos = 0;
    private int lastIndexPos = 0;
    private long curEid = -1;

    public VarLenValueFilePackIterator(PeekingIterator<ETVEntry> iterator, int segmentSize, int blockSize)
    {
        this.iter = iterator;
        this.approximateSegmentSize = segmentSize;
        this.blockSize = blockSize;
        try
        {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    protected Slice computeNext()
    {
        if(!iter.hasNext()) return endOfData();

        ByteBufferDataOutput buffer = new ByteBufferDataOutput();

        while(iter.hasNext() && buffer.size()<approximateSegmentSize)
        {
            ETVEntry entry = iter.next();

            int curPos = buffer.position()+startPos;

            if(curEid==-1 || curEid!=entry.getEntityId() || curPos==0 || curPos-lastIndexPos>=blockSize){
                indexSegment.writeLong(entry.getEntityId());
                indexSegment.write(entry.getTime().encode());
                indexSegment.writeInt( curPos );
                lastIndexPos = curPos;
            }

            curEid = entry.getEntityId();

            buffer.write(entry.getTime().encode());
            buffer.write(entry.getValue().encode());
        }

        startPos += buffer.size();
        md.update(buffer.toByteArray());
        return buffer.getSlice();
    }

    public Slice getIndex(){
        return indexSegment.getSlice();
    }

    public MessageDigest getMD5(){
        return md;
    }

}
