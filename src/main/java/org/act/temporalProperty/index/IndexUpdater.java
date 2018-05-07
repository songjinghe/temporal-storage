package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.InternalEntry;

import java.util.List;

/**
 * update index for one or more (multi-property time value index) property.
 * should update all index relevant.
 * first setMergeInfo() is called, then update called many times, then updateMeta() once, cleanUp() once.
 *
 * Created by song on 2018-05-06.
 */
public class IndexUpdater
{
    private List<IndexFileMeta> fileToDelete;
    private long fileId;
    private IndexMetaManager meta;
    private int proId;
    private boolean isStable;

    public IndexUpdater( IndexMetaManager meta, int propertyId, boolean isStable, long fileId, List<IndexFileMeta> fileToUpdate )
    {
        this.meta = meta;
        this.proId = propertyId;
        this.isStable = isStable;
        this.fileId = fileId;
        this.fileToDelete = fileToUpdate;
        this.
    }

    public IndexUpdater()
    {
    }

    public void update( InternalEntry entry )
    {

    }

    public void updateMeta()
    {

    }

    public void cleanUp()
    {

    }


}
