package org.act.temporalProperty.index;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by song on 2018-01-24.
 */
public class LevelMergeIterator extends AbstractSeekingIterator<Slice, Slice> {
    private final List<? extends SeekingIterator<Slice, Slice>> levels;

    public LevelMergeIterator(SeekingIterator<Slice, Slice> ... iterators ){
        levels = Arrays.asList(iterators);
    }

    public LevelMergeIterator(){
        levels = new ArrayList<>();
    }

    @Override
    protected void seekToFirstInternal() {
        //
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        //
    }

    @Override
    protected Map.Entry<Slice, Slice> getNextElement() {
        return null;
    }

    public void add(SeekingIterator<Slice,Slice> appendIterator) {

    }
}
