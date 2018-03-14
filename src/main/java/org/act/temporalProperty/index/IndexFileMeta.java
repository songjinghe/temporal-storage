package org.act.temporalProperty.index;

import java.util.List;

/**
 * Created by song on 2018-01-19.
 */
public class IndexFileMeta {

    private final long no;
    private final long size;
    private final List<IndexQueryRegion> regions;

    public IndexFileMeta(long number, long fileSize, List<IndexQueryRegion> regions) {
        this.no = number;
        this.size = fileSize;
        this.regions = regions;
    }

    public long getNo() {
        return no;
    }

    public long getSize() {
        return size;
    }

    public List<IndexQueryRegion> getRegions() {
        return regions;
    }
}
