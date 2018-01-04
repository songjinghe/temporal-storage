package edu.buaa.act.temporal.io;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.exception.TPSRuntimeException;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Created by song on 2018-01-03.
 */
public class PackToBlockIterator extends AbstractIterator<byte[]> implements PeekingIterator<byte[]>
{
    private final PeekingIterator<byte[]> in;
    private final CRC32 crc32 = new CRC32();

    private final int blockSize;
    private final int headerSize;
    private final int contentMaxSize;

    private ByteBuffer toProcess;

    public PackToBlockIterator(PeekingIterator<byte[]> input, int blockSize)
    {
        this.in = input;
        this.blockSize = blockSize;
        this.headerSize = 8 + 4; // 8 for crc32 value, 4 for content length
        this.contentMaxSize = blockSize - headerSize;
    }

    @Override
    protected byte[] computeNext()
    {
        ByteBuffer block = ByteBuffer.allocate(blockSize);
        block.clear();
        block.putLong(0);// placeholder for crc32
        block.putInt(0);// placeholder for content length

        while (block.hasRemaining())
        {
            ByteBuffer inData;

            if(toProcess!=null)
            {
                inData = toProcess;
                toProcess = null;
            }
            else if(in.hasNext())
            {
                inData = ByteBuffer.wrap(in.next());
                inData.flip();
            }
            else if(block.position()>headerSize) // contains data
            {
                return finish(block);
            }else{
                return endOfData();
            }

            int remain = block.remaining();

            if (inData.remaining() > remain)
            {
                this.toProcess = inData;
                block.put(inData.array(), inData.position(), remain);
                inData.position(inData.position()+remain);
                return finish(block);
            }
            else if(inData.remaining() == remain)
            {
                block.put(inData);
                return finish(block);
            }
            else // inData.length < contentMaxSize
            {
                block.put(inData);
            }
        }
        // block full, already handled, should not happen.
        throw new TPSRuntimeException("SNH: block full");
    }

    private byte[] finish(ByteBuffer buffer)
    {
        byte[] content = buffer.array();
        crc32.update(content, 8+4, contentMaxSize);
        buffer.putLong(0, crc32.getValue()); // write back crc32
        buffer.putInt(8, buffer.position()-8-4); // write back content length
        crc32.reset();
        return content;
    }
}


























