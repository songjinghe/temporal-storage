package org.act.temporalProperty.index;

import org.act.temporalProperty.util.Slice;

import java.nio.ByteBuffer;

/**
 * Created by song on 2018-01-19.
 */
public class TimePointEntry {
    private int propertyId;
    private long entityId;
    private int timePoint;
    private Slice value;

    public TimePointEntry(int propertyId, long entityId, int timePoint, Slice value) {
        this.propertyId = propertyId;
        this.entityId = entityId;
        this.timePoint = timePoint;
        this.value = value;
    }

    public int getPropertyId() {
        return propertyId;
    }

    public long getEntityId() {
        return entityId;
    }

    public int getTimePoint() {
        return timePoint;
    }

    public Slice getValue() {
        return value;
    }
}
