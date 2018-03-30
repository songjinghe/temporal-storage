package org.act.temporalProperty.meta;

import org.act.temporalProperty.impl.FileMetaData;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.util.Collection;

/**
 * Created by song on 2018-01-17.
 */
public class PropertyMetaDataController {

    public static PropertyMetaData decode(Slice data) {
        return decode(data.input());
    }

    public static PropertyMetaData decode(SliceInput in) {
        int pid = in.readInt();
        ValueContentType type = ValueContentType.decode(in.readInt());
        PropertyMetaData p = new PropertyMetaData(pid, type);
        int count = in.readInt();
        for(int i=0; i<count; i++){
            FileMetaData file = FileMetaDataController.decode(in);
            p.addUnstable(file);
        }
        count = in.readInt();
        for(int i=0; i<count; i++){
            FileMetaData file = FileMetaDataController.decode(in);
            p.addStable(file);
        }
        return p;
    }

    public static Slice encode(PropertyMetaData meta) {
        DynamicSliceOutput out = new DynamicSliceOutput(40);
        encode(out, meta);
        return out.slice();
    }

    public static void encode(SliceOutput out, PropertyMetaData meta) {
        out.writeInt(meta.getPropertyId());//4
        out.writeInt(meta.getType().getId());//4
        Collection<FileMetaData> unstable = meta.getUnStableFiles().values();
        out.writeInt(unstable.size());//4
        for(FileMetaData fMeta : unstable){
            FileMetaDataController.encode(out, fMeta);//24
        }
        Collection<FileMetaData> stable = meta.getStableFiles().values();
        out.writeInt(stable.size());//4
        for(FileMetaData fMeta : stable){
            FileMetaDataController.encode(out, fMeta);//24
        }
    }
}
