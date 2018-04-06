package org.act.temporalProperty.index;

import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-01-27.
 */
public class EntityTimeIntervalEntry {
    private final long entityId;
    private final int start;
    private final int end;
    private final Slice value;
    public EntityTimeIntervalEntry(long entityId, int start, int end, Slice value){
        this.entityId = entityId;
        this.start = start;
        this.end = end;
        this.value = value;
    }

    public long entityId(){
        return entityId;
    }

    public int start(){
        return this.start;
    }

    public int end(){
        return this.end;
    }

    public Slice value(){
        return this.value;
    }
}
