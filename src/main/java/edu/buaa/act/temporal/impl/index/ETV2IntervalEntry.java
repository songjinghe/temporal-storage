package edu.buaa.act.temporal.impl.index;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimeValueEntry;
import edu.buaa.act.temporal.impl.table.ETVEntry;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by song on 2018-01-09.
 */
public class ETV2IntervalEntry extends AbstractIterator<TimeValueEntry> implements PeekingIterator<TimeValueEntry>
{
    private List<PeekingIterator<ETVEntry>> in;

    public ETV2IntervalEntry(PeekingIterator<ETVEntry> iterator){
        this.in = new LinkedList<>();
        this.in.add(iterator);
    }

    public ETV2IntervalEntry(List<PeekingIterator<ETVEntry>> iterators){
        this.in = iterators;
    }


}
