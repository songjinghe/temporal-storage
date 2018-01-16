package edu.buaa.act.temporal.exception;

import edu.buaa.act.temporal.TimePoint;

/**
 * Created by song on 2018-01-15.
 *
 * happens when TimePoint reach INT_MAX=2^31-1
 */
public class TimeOverFlowException extends TPSRuntimeException
{
    public TimeOverFlowException(TimePoint t)
    {
        super(t.toString());
    }
}
