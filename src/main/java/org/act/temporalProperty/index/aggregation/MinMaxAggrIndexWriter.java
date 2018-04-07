package org.act.temporalProperty.index.aggregation;

import org.act.temporalProperty.impl.Options;
import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.table.TableBuilder;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.util.Slice;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;

import static org.act.temporalProperty.index.IndexType.AGGR_MAX;
import static org.act.temporalProperty.index.IndexType.AGGR_MIN;
import static org.act.temporalProperty.index.IndexType.AGGR_MIN_MAX;

/**
 * Created by song on 2018-04-05.
 */
public class MinMaxAggrIndexWriter {

    private final File file;
    private final Comparator<Slice> cp;
    private final Map<Pair<Long,Integer>,Slice> min = new TreeMap<>();
    private final Map<Pair<Long,Integer>,Slice> max = new TreeMap<>();
    private final boolean buildMin;
    private final boolean buildMax;

    public MinMaxAggrIndexWriter(List<Triple<Long,Integer,Slice>> data, File file, Comparator<Slice> valCp, IndexType type) {
        this.file = file;
        this.cp = valCp;
        buildMin = (type==AGGR_MIN || type==AGGR_MIN_MAX);
        buildMax = (type==AGGR_MAX || type==AGGR_MIN_MAX);
        for(Triple<Long,Integer,Slice> entry : data){
            Pair<Long, Integer> key = Pair.of(entry.getLeft(), entry.getMiddle());
            if(buildMin) min.merge(key, entry.getRight(), this::min);
            if(buildMax) max.merge(key, entry.getRight(), this::max);
        }
    }

    public long write() throws IOException {
        try(FileOutputStream targetStream = new FileOutputStream(file)) {
            FileChannel targetChannel = targetStream.getChannel();
            TableBuilder builder = new TableBuilder(new Options(), targetChannel, TableComparator.forAggrIndex());
            for(Map.Entry<Pair<Long,Integer>,Slice> entry : min.entrySet()){
                Pair<Long,Integer> key = entry.getKey();
                if(buildMin) builder.add(toSlice(key), entry.getValue());
                if(buildMax) builder.add(toSlice(key), max.get(key));
            }
            targetChannel.force(true);
            return targetChannel.size();
        }
    }

    private Slice min(Slice a, Slice b){
        if(cp.compare(a, b)<=0){
            return a;
        }else{
            return b;
        }
    }

    private Slice max(Slice a, Slice b){
        if(cp.compare(a, b)>=0){
            return a;
        }else{
            return b;
        }
    }

    private Slice toSlice(Pair<Long,Integer> key){
        long entityId = key.getLeft();
        int timeGroupId = key.getRight();
        Slice s = new Slice(12);
        s.setLong(0, entityId);
        s.setInt(8, timeGroupId);
        return s;
    }
}
