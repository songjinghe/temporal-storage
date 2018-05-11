package org.act.temporalProperty.index;

/**
 * Created by song on 2018-05-06.
 */
public class IndexFileMeta
{
    private final long indexId;
    private final long fileId;
    private final long fileSize;
    private final int startTime;
    private final int endTime;

    //corresponding storage file properties, for single-property time value index and aggregation index.
    private long corFileId;
    private boolean corIsStable;

    public IndexFileMeta( long indexId, long fileId, long fileSize, int startTime, int endTime, long corFileId, Boolean corIsStable )
    {
        this.indexId = indexId;
        this.fileId = fileId;
        this.fileSize = fileSize;
        this.startTime = startTime;
        this.endTime = endTime;
        this.corFileId = corFileId;
        this.corIsStable = corIsStable;
    }

    public long getIndexId()
    {
        return indexId;
    }

    public long getFileId()
    {
        return fileId;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public int getStartTime()
    {
        return startTime;
    }

    public int getEndTime()
    {
        return endTime;
    }

    public long getCorFileId()
    {
        return corFileId;
    }

    public boolean isCorIsStable()
    {
        return corIsStable;
    }
}
