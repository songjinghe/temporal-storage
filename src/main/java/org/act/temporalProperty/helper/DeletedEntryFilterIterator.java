package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * This iterator just skip those entries which is marked as DELETION.
 * The output of this iterator contains NO entries with a DELETION mark.
 * Created by song on 2018-03-28.
 */
public class DeletedEntryFilterIterator extends AbstractIterator<Entry<Slice,Slice>> implements SeekingIterator<Slice,Slice> {

    private final SeekingIterator<Slice, Slice> in;

    public DeletedEntryFilterIterator(SeekingIterator<Slice,Slice> input){
        this.in = input;
    }

    @Override
    protected Entry<Slice, Slice> computeNext() {
        while(in.hasNext()){
            Entry<Slice, Slice> entry = in.next();
            InternalKey key = new InternalKey(entry.getKey());
            if(key.getValueType() != ValueType.DELETION) return entry;
        }
        return endOfData();
    }

    @Override
    public void seekToFirst() {
        in.seekToFirst();
    }

    @Override
    public void seek(Slice targetKey) {
        in.seek(targetKey);
    }
}
