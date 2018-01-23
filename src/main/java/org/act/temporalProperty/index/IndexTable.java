package org.act.temporalProperty.index;

import org.act.temporalProperty.index.rtree.IndexEntryOperator;
import org.act.temporalProperty.util.Slice;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by song on 2018-01-19.
 */
public class IndexTable {

    private final FileChannel channel;

    public IndexTable(FileChannel fileChannel) {
        this.channel = fileChannel;
    }

    public Iterator<Slice> iterator(IndexQueryRegion regions) throws IOException {
        return new IndexTableIterator(this.channel, regions, extractOperator(regions));
    }

    private IndexEntryOperator extractOperator(IndexQueryRegion regions) {
        List<IndexValueType> types = new ArrayList<>();
        for(PropertyValueInterval p : regions.getPropertyValueIntervals()){
            types.add(p.getType());
        }
        return new IndexEntryOperator(types, 4096);
    }
}
