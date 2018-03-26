package org.act.temporalProperty.index;

import org.act.temporalProperty.index.IndexMetaData.*;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by song on 2018-01-18.
 */
public class IndexMetaDataController {
    public static Slice encode(IndexMetaData meta){
        DynamicSliceOutput out = new DynamicSliceOutput(128);
        encode(out, meta);
        return out.slice();
    }

    public static void encode(SliceOutput out, IndexMetaData meta){
        out.writeInt(meta.getType().getId());
        out.writeInt(meta.getTimeStart());
        out.writeInt(meta.getTimeEnd());
        out.writeInt(meta.getPropertyIdList().size());
        for(Integer pid : meta.getPropertyIdList()){
            out.writeInt(pid);
        }
    }

    public static IndexMetaData decode(SliceInput in){
        int id = in.readInt();
        IndexType tid = IndexType.decode(in.readInt());
        int start = in.readInt();
        int end = in.readInt();
        int count = in.readInt();
        List<Integer> pidList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            pidList.add(in.readInt());
        }
        return new IndexMetaData(id, tid, pidList, start, end);
    }

    public static IndexMetaData decode(Slice in){
        return decode(in.input());
    }
}
