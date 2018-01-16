package edu.buaa.act.temporal.impl.index;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.exception.TPSNHException;
import edu.buaa.act.temporal.impl.ValueOperator;
import edu.buaa.act.temporal.impl.table.ETVEntry;


/**
 * Created by song on 2018-01-14.
 *
 * convert an ETVEntry iterator to SingleValueIndexDataEntry iterator
 */
public class SingleValueIndexDataEntryFactory extends AbstractIterator<SingleValueIndexDataEntry> implements PeekingIterator<SingleValueIndexDataEntry>
{
    private final PeekingIterator<ETVEntry> in;
    private final TimePoint end;
    private final ValueOperator valOp;

    public SingleValueIndexDataEntryFactory(PeekingIterator<ETVEntry> iterator, TimePoint end, ValueOperator valueOperator)
    {
        this.in = iterator;
        this.end = end;
        this.valOp = valueOperator;
    }

    @Override
    protected SingleValueIndexDataEntry computeNext()
    {
        if(in.hasNext())
        {
            ETVEntry tpv1 = in.next();
            if(in.hasNext())
            {
                ETVEntry tpv2 = in.peek();
                if(tpv1.getEntityId()==tpv2.getEntityId())
                {
                    if(tpv2.getTime().hasPre())
                    {
                        return new SingleValueIndexDataEntry(tpv1.getTime(), tpv2.getTime().pre(), tpv1.getEntityId(), tpv1.getValue(), valOp);
                    }else{
                        throw new TPSNHException("SNH: time not have pre");
                    }
                }else{
                    return new SingleValueIndexDataEntry(tpv1.getTime(), end, tpv1.getEntityId(), tpv1.getValue(), valOp);
                }
            }else{
                return new SingleValueIndexDataEntry(tpv1.getTime(), end, tpv1.getEntityId(), tpv1.getValue(), valOp);
            }
        }else{
            return endOfData();
        }
    }
}
