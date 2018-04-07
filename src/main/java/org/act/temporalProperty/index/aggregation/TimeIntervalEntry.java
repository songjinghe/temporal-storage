package org.act.temporalProperty.index.aggregation;

import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-01-27.
 */
public class TimeIntervalEntry {
    private final int start;
    private final int end;
    private final Slice value;
    public TimeIntervalEntry(int start, int end, Slice value){
        this.start = start;
        this.end = end;
        this.value = value;
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
