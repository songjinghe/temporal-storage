package org.act.temporalProperty.index;

import org.act.temporalProperty.EPEntryIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by song on 2018-01-24.
 * this class is used in range query for unstable file to build a combined iterator
 * 注意：不同文件的时间虽然无overlap，但内部Key(entity id, pro Id, time)是有overlap的
 * should call seek() or seekToFirst() to initialize all sub-iterators.
 */
public class EPAppendIterator extends AppendIterator{
    // each sub iterator's time should be inc (e.g. 0 is the earliest time)
    private List<SeekingIterator<Slice,Slice>> iterators = new ArrayList<>();
    private int cur = 0;
    private Slice id;

    public EPAppendIterator(Slice idSlice) {
        this.id = idSlice;
    }

    public void append(EPEntryIterator iterator) {
        iterators.add(new EPEntryIterator(id, iterator));
    }

    @Override
    protected void seekToFirstInternal() {
        for(SeekingIterator<Slice, Slice> iterator : iterators) {
            iterator.seekToFirst();
        }
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        checkIfValidKey(targetKey);
        cur=0;
        while(cur<iterators.size()){
            SeekingIterator<Slice,Slice> iterator = iterators.get(cur);
            iterator.seek(targetKey);
            if(iterator.hasNext()){
                return;
            }else{
                cur++;
            }
        }
    }

    private void checkIfValidKey(Slice targetKey) {
        InternalKey target = new InternalKey(targetKey);
        if(!target.getId().equals(id)) throw new IllegalArgumentException("target should has same entity id and same property id");
    }

    @Override
    protected Map.Entry<Slice, Slice> getNextElement() {
        while(cur<iterators.size()){
            if(iterators.get(cur).hasNext()){
                return iterators.get(cur).next();
            }else{
                cur++;
            }
        }
        return null;
    }

    public int size() {
        return iterators.size();
    }
}
