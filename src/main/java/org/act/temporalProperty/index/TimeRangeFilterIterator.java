package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by song on 2018-01-25.
 */
public class TimeRangeFilterIterator extends AbstractSeekingIterator<Slice, Slice>{

    private final SeekingIterator<Slice, Slice> in;
    private final int start;
    private final int end;

    public TimeRangeFilterIterator(SeekingIterator<Slice,Slice> in, int start, int end){
        this.in = in;
        this.start = start;
        this.end = end;
    }

    @Override
    protected void seekToFirstInternal() {
        in.seekToFirst();
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        in.seek(targetKey);
    }

    @Override
    protected Entry<Slice, Slice> getNextElement() {
        while(in.hasNext()){
            Entry<Slice, Slice> entry = in.next();
            InternalKey key = new InternalKey(entry.getKey());
            if(start<=key.getStartTime() && key.getStartTime()<=end){
                return entry;
            }
        }
        return null;
    }
}
