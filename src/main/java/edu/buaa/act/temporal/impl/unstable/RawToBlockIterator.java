package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.iterator.SearchableIterator;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Created by song on 2018-01-04.
 */
public class RawToBlockIterator extends AbstractIterator<ByteBuffer> implements PeekingIterator<ByteBuffer>
{
    private final ByteBuffer in;
    private final CRC32 crc32 = new CRC32();

    public RawToBlockIterator(ByteBuffer in){
        this.in = in;
        this.in.flip();
    }

    @Override
    protected ByteBuffer computeNext()
    {
        if(in.hasRemaining()){
            if(in.remaining()>=4*1024){
                return getNextBlock(4*1024);
            }else{
                return getNextBlock(in.remaining());
            }
        }else{
            return endOfData();
        }
    }

    private ByteBuffer getNextBlock(int length)
    {
        crc32.reset();
        ByteBuffer block = ByteBuffer.wrap(this.in.array(), this.in.arrayOffset()+this.in.position(), length);
        block.flip();
        long crc = block.getLong();
        int contentLen = block.getInt();
        if(contentLen!=block.remaining()){
            throw new TPSRuntimeException("content length not match");
        }
        crc32.update(block);
        if(crc!=crc32.getValue()){
            throw new TPSRuntimeException("checksum not match");
        }
        block.flip();
        return block;
    }


}
