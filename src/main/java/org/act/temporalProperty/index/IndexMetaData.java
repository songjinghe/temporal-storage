package org.act.temporalProperty.index;

import java.util.List;

/**
 * Created by song on 2018-01-17.
 */
public class IndexMetaData {
    private long fileId;
    private IndexType type;
    private List<Integer> propertyIdList;
    private int timeStart;
    private int timeEnd;
    private long fileSize;

    public IndexMetaData(long fileId, IndexType type, List<Integer> pidList, int start, int end, long fileSize) {
        this.fileId = fileId;
        this.type = type;
        this.propertyIdList = pidList;
        this.timeStart = start;
        this.timeEnd = end;
        this.fileSize = fileSize;
    }

    public long getId() {
        return fileId;
    }

    public IndexType getType() {
        return type;
    }

    public int getTimeStart() {
        return timeStart;
    }

    public int getTimeEnd() {
        return timeEnd;
    }

    public List<Integer> getPropertyIdList(){
        return propertyIdList;
    }

    public long getFileSize() {
        return fileSize;
    }

    @Override
    public String toString() {
        return "IndexMetaData{" +
                "fileId=" + fileId +
                ", type=" + type +
                ", propertyIdList=" + propertyIdList +
                ", timeStart=" + timeStart +
                ", timeEnd=" + timeEnd +
                ", fileSize=" + fileSize +
                '}';
    }
}
