package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.AbstractSeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by song on 2018-01-24.
 *
 * when adding sub iterators of unstable files, should always add from earliest to latest (time is inc)
 *
 */
public class AppendIterator extends AbstractSeekingIterator<Slice,Slice>{
    private List<SeekingIterator<Slice,Slice>> iterators = new ArrayList<>();
    private int cur = 0;

    public void append(SeekingIterator<Slice, Slice> iterator) {
        if(iterator.hasNext()){
            iterators.add(iterator);
            iterators.sort((o1, o2) -> {
                InternalKey key1 = new InternalKey(o1.peek().getKey());
                InternalKey key2 = new InternalKey(o2.peek().getKey());
                return key1.getStartTime()-key2.getStartTime();
            });
        }

    }

    @Override
    protected void seekToFirstInternal() {
        cur = 0;
        for(SeekingIterator<Slice,Slice> iterator : iterators){
            iterator.seekToFirst();
        }
    }

    @Override
    protected void seekInternal(Slice targetKey) {
        cur=0;
        for(int i=0; i<iterators.size(); i++){
            SeekingIterator<Slice,Slice> iterator = iterators.get(cur);
            iterator.seek(targetKey);
            if(!iterator.hasNext()){
                cur++;
            }else{
                break;
            }
        }
        for(int i=cur; i<iterators.size(); i++){
            SeekingIterator<Slice,Slice> iterator = iterators.get(i);
            iterator.seekToFirst();
        }
    }

    @Override
    protected Map.Entry<Slice, Slice> getNextElement() {
        while(cur<iterators.size()){
            if(iterators.get(cur).hasNext()) {
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
