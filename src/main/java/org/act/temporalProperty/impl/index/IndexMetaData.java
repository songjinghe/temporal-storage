package org.act.temporalProperty.impl.index;

import java.util.List;

/**
 * Created by song on 2018-01-17.
 */
public class IndexMetaData {
    private int id;
    private IndexType type;
    private List<Integer> propertyIdList;
    private int timeStart;
    private int timeEnd;

    public IndexMetaData(int id, IndexType type, List<Integer> propertyIdList, int timeStart, int timeEnd) {
        this.id = id;
        this.type = type;
        this.propertyIdList = propertyIdList;
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
    }

    public int getId() {
        return id;
    }

    public IndexType getType() {
        return type;
    }

    public List<Integer> getPropertyIdList() {
        return propertyIdList;
    }

    public int getTimeStart() {
        return timeStart;
    }

    public int getTimeEnd() {
        return timeEnd;
    }
}
