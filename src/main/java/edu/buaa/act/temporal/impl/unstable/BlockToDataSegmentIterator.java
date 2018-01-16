package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.helper.ByteBufferDataOutput;

import java.nio.ByteBuffer;

/**
 * Created by song on 2018-01-04.
 */
public class BlockToDataSegmentIterator extends AbstractIterator<ByteBuffer> implements PeekingIterator<ByteBuffer>
{
    private final PeekingIterator<ByteBuffer> in;
    private final int minDataSegSize;

    public BlockToDataSegmentIterator(PeekingIterator<ByteBuffer> in, int segmentSize){
        this.in = in;
        this.minDataSegSize = segmentSize;
    }

    @Override
    protected ByteBuffer computeNext()
    {
        ByteBufferDataOutput out = new ByteBufferDataOutput();
        boolean hasData = false;
        while(out.size()<minDataSegSize)
        {
            if(in.hasNext()){
                ByteBuffer block = in.next();
                block.getLong();
                block.getInt();
                out.write(block.array(), block.arrayOffset()+block.position(), block.remaining());
                hasData = true;
            }else if(hasData){
                break;
            }else{
                return endOfData();
            }
        }
        return ByteBuffer.wrap(out.toByteArray());
    }
}
