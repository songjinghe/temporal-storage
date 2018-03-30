package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by song on 2018-01-24.
 * this class is used in range query for disk file to build a combined iterator of one entity.
 * the result iterator only contains entries for one entity, and its time is always inc when iterating
 * when adding sub iterators, should always add from earliest to latest (time is inc)
 * 注意：不同文件的时间虽然无overlap，但内部Key(entity id, pro Id, time)是有overlap的
 * should call seek() or seekToFirst() to initialize all sub-iterators.
 */
public class EPAppendIterator extends AbstractIterator<InternalEntry> implements SearchableIterator {
    // each sub iterator's time should be inc (e.g. 0 is the earliest time)
    private List<SearchableIterator> iterators = new ArrayList<>();
    private int cur = 0;
    private Slice id;

    public EPAppendIterator(Slice idSlice) {
        this.id = idSlice;
    }

    public void append(SearchableIterator iterator) {
        if(isEP(iterator)){
            iterators.add(iterator);
        }else {
            iterators.add(new EPEntryIterator(id, iterator));
        }
    }

    private boolean isEP(SearchableIterator iterator) {
        return  (iterator instanceof EPEntryIterator) ||
                (iterator instanceof EPAppendIterator) ||
                (iterator instanceof EPMergeIterator);
    }

    @Override
    public void seekToFirst() {
        for(SearchableIterator iterator : iterators) {
            iterator.seekToFirst();
        }
    }

    @Override
    public void seek(InternalKey targetKey) {
        checkIfValidKey(targetKey);
        cur=0;
        while(cur<iterators.size()){
            SearchableIterator iterator = iterators.get(cur);
            iterator.seek(targetKey);
            if(iterator.hasNext()){
                return;
            }else{
                cur++;
            }
        }
    }

    private void checkIfValidKey(InternalKey target) {
        if(!target.getId().equals(id)) throw new IllegalArgumentException("target should has same entity id and same property id");
    }


    public int size() {
        return iterators.size();
    }

    @Override
    protected InternalEntry computeNext() {
        while(cur<iterators.size()){
            if(iterators.get(cur).hasNext()){
                return iterators.get(cur).next();
            }else{
                cur++;
            }
        }
        return endOfData();
    }
}
