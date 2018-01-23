package org.act.temporalProperty.index;


import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-01-21.
 */
public class IndexIntervalEntry {
    private long entityId;
    private int start;
    private int end;
    private Slice value;

    public IndexIntervalEntry(long entityId, int start, int end, Slice value) {
        this.entityId = entityId;
        this.start = start;
        this.end = end;
        this.value = value;
    }

    public long getEntityId() {
        return entityId;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public Slice getValue() {
        return value;
    }

}
