package org.act.temporalProperty.index;

import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.index.rtree.IndexEntryOperator;

/**
 * Created by song on 2018-01-27.
 */
public class TimeIntervalEntry {
    private final IndexEntryOperator op;
    private final IndexEntry indexEntry;
    public TimeIntervalEntry(IndexEntry entry, IndexEntryOperator op){
        this.indexEntry = entry;
        this.op = op;
    }

    public int start(){
        return indexEntry.getStart();
    }

    public int end(){
        return indexEntry.getEnd();
    }

    public int value(int i){
        return 0;
    }
}
