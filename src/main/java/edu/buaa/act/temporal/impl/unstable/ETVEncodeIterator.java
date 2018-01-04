package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.helper.ByteBufferDataOutput;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by song on 2018-01-02.
 *
 * returned byte[] contain at least one entity's data, or multi entity's data.
 * and its size >= approximateBlockSize unless there is no more data.
 *
 */
public class ETVEncodeIterator extends AbstractIterator<byte[]> implements PeekingIterator<byte[]>
{
    private final int approximateBlockSize; // returned byte[] 's length always <= blockSize
    private final PeekingIterator<ETVEntry> iter;
    private final TreeMap<Integer, Integer> writeLater = new TreeMap<>();


    public ETVEncodeIterator(PeekingIterator<ETVEntry> iterator, int blockSize)
    {
        this.iter = iterator;
        this.approximateBlockSize = blockSize;
    }

    @Override
    protected byte[] computeNext()
    {
        if(!iter.hasNext()) return endOfData();

        ByteBufferDataOutput buffer = new ByteBufferDataOutput();

        long lastEntityId = -1;
        int lengthPos = -1;

        while(iter.hasNext())
        {
            ETVEntry entry = iter.peek();
            long curEID = entry.getEntityId();

            if(lastEntityId==-1)
            {
                buffer.writeLong(curEID);
                lengthPos = buffer.position();
                buffer.writeInt(0); // placeholder, eid length inside block
                buffer.write(entry.getTime().encode());
                buffer.write(entry.getValue().encode());
            }
            else if(curEID==lastEntityId)
            {
                buffer.write(entry.getTime().encode());
                buffer.write(entry.getValue().encode());
            }
            else // lastEntityId!=-1 && lastEntityId!=curEID
            {
                writeLater(lengthPos, buffer.position() - lengthPos); // fill back length

                if(buffer.size()>approximateBlockSize) return completeWrite(buffer);
                else
                {
                    buffer.writeLong(curEID);
                    lengthPos = buffer.position();
                    buffer.writeInt(0);// placeholder, eid length inside block
                    buffer.write(entry.getTime().encode());
                    buffer.write(entry.getValue().encode());
                }
            }
            lastEntityId = curEID;
            iter.next();
        }

        return completeWrite(buffer);
    }

    private void writeLater(int pos, int value)
    {
        this.writeLater.put(pos, value);
    }

    private byte[] completeWrite(ByteBufferDataOutput buffer)
    {
        ByteBuffer content = ByteBuffer.wrap(buffer.toByteArray());

        for(Map.Entry<Integer, Integer> e : writeLater.entrySet())
        {
            content.putInt(e.getKey(), e.getValue());
        }
        writeLater.clear();
        return content.array();
    }

}
