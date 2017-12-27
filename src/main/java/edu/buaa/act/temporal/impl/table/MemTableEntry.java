package edu.buaa.act.temporal.impl.table;

import edu.buaa.act.temporal.TimeInterval;
import edu.buaa.act.temporal.TimeValueEntry;
import edu.buaa.act.temporal.ValueAtTime;

/**
 * Created by song on 17-12-11.
 */
public class MemTableEntry extends TimeValueEntry
{
    private int propertyId;
    private long entityId;


    public MemTableEntry(int propertyId, long entityId, TimeInterval time, ValueAtTime value) {
        super(time, value);
        this.entityId = entityId;
        this.propertyId = propertyId;
    }

    public int getPropertyId() {
        return propertyId;
    }

    public long getEntityId() {
        return entityId;
    }
}
