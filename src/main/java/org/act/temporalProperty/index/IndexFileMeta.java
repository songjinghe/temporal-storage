package org.act.temporalProperty.index;

/**
 * Created by song on 2018-05-06.
 */
public class IndexFileMeta
{
    private long indexId;
    private long fileId;
    private long fileSize;
    private int startTime;
    private int endTime;

    //corresponding storage file properties, for single-property time value index and aggregation index.
    private long corFileId;
    private boolean corIsStable;

    public IndexFileMeta( long indexId, long fileId, long fileSize, long corFileId, Boolean corIsStable )
    {
        this.indexId = indexId;
        this.fileId = fileId;
        this.fileSize = fileSize;
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
