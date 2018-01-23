package org.act.temporalProperty.index;

import org.act.temporalProperty.util.Slice;

import java.nio.ByteBuffer;

/**
 * Created by song on 2018-01-19.
 */
public class IndexPointEntry {
    private long entityId;
    private int timePoint;
    private Slice value;

    public IndexPointEntry(long entityId, int timePoint, Slice value) {
        this.entityId = entityId;
        this.timePoint = timePoint;
        this.value = value;
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
