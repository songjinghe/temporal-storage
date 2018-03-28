package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

/**
 * Created by song on 2018-03-28.
 */
public abstract class PairViewFilterIterator<T> extends AbstractIterator<T> {
    private final Iterator<T> in;
    private T cur;
    private T next;

    public PairViewFilterIterator(Iterator<T> in){
        this.in = in;
        if(in.hasNext()) cur=in.next();
        if(in.hasNext()) next=in.next();
    }

    @Override
    protected T computeNext() {
        while(cur!=null && next!=null){
            if(shouldReturnFirst(cur, next)){
                return shift2next();
            }
        }
        if(cur!=null){ // next==null
            assert next==null:"next should be null!";
            if(shouldReturnFirst(cur, null)){
                return shift2next();
            }else{
                return endOfData();
            }
        }else{
            return endOfData();
        }
    }

    private T shift2next() {
        T tmp = cur;
        cur = next;
        if(in.hasNext()){
            next = in.next();
        }else{
            next = null;
        }
        return tmp;
    }

    abstract protected boolean shouldReturnFirst(T cur, T next);

}
