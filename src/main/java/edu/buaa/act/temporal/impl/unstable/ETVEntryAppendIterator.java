package edu.buaa.act.temporal.impl.unstable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by song on 2018-01-02.
 */
public class ETVEntryAppendIterator extends AbstractIterator<ETVEntry> implements PeekingIterator<ETVEntry>
{
    private final LinkedList<Iterator<ETVEntry>> iteratorList;

    public ETVEntryAppendIterator(List<Iterator<ETVEntry>> input)
    {
        this.iteratorList = new LinkedList<>(input);
    }

    public ETVEntryAppendIterator(){
        this.iteratorList = new LinkedList<>();
    }

    public void append(Iterator<ETVEntry> input)
    {
        this.iteratorList.add(input);
    }

    public void addFirst(Iterator<ETVEntry> input)
    {
        this.iteratorList.addFirst(input);
    }

    @Override
    protected ETVEntry computeNext()
    {
        Iterator<ETVEntry> curIter;
        while(true)
        {
            curIter = iteratorList.peek();
            if(curIter!=null){
                if(curIter.hasNext()){
                    return curIter.next();
                }else{
                    iteratorList.poll();
                }
            }else{
                return endOfData();
            }
        }
    }

    public int size(){
        return iteratorList.size();
    }

    public boolean isEmpty(){ return iteratorList.isEmpty();}
}
