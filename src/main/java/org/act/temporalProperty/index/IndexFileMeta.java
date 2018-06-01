package org.act.temporalProperty.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;

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

    //time group start point for aggregation index only. the last point is endTime + 1
    private final List<Integer> timeGroup;

    public IndexFileMeta( long indexId, long fileId, long fileSize, int startTime, int endTime, long corFileId, Boolean corIsStable, Collection<Integer> timeGroup )
    {
        this.indexId = indexId;
        this.fileId = fileId;
        this.fileSize = fileSize;
        this.startTime = startTime;
        this.endTime = endTime;
        this.corFileId = corFileId;
        this.corIsStable = corIsStable;
        this.timeGroup = new ArrayList<>();
        this.timeGroup.addAll( timeGroup );
        this.timeGroup.sort( Comparator.naturalOrder() );
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

    public Collection<Integer> getTimeGroups()
    {
        return timeGroup;
    }
}
