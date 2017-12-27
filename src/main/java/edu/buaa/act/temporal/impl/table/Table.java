package edu.buaa.act.temporal.impl.table;

import edu.buaa.act.temporal.TimePoint;

import java.io.DataOutput;

/**
 * Created by song on 17-12-6.
 */
public class Table
{
    private int id;
    private int propertyId;
    private TimePoint start;
    private TimePoint end;
    private boolean isStable;
    private boolean hasBuffer;

    public void encode(DataOutput out)
    {

    }
}
