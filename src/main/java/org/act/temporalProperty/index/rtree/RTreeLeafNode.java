package org.act.temporalProperty.index.rtree;

import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by song on 2018-01-19.
 *
 * This class is a simple wrapper of Slice
 */
public class RTreeLeafNode extends RTreeNode {
    private List<Slice> entry;

    public RTreeLeafNode(List<Slice> entry, IndexEntryOperator op) {
        this.entry = entry;
        this.updateBound(op);
    }

    public RTreeLeafNode(List<Slice> data) {
        this.entry = data;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public List<Slice> getEntries() {
        return entry;
    }

    @Override
    public void encode(SliceOutput out) {
        List<Slice> data = this.getEntries();
        out.writeInt(data.size());
        for(Slice entry: data){
            out.writeInt(entry.length());
            out.writeBytes(entry);
        }
    }

    public static RTreeLeafNode decode(SliceInput in){
        int count = in.readInt();
        List<Slice> data = new ArrayList<>();
        for(int i=0; i<count; i++){
            int len = in.readInt();
            Slice content = in.readSlice(len);
            data.add(content);
        }
        return new RTreeLeafNode(data);
    }

    private void updateBound(IndexEntryOperator op){
        if(this.entry.size()>0) {
            this.bound = new RTreeRange(this.entry, op);
        }
    }

    @Override
    public List<RTreeNode> getChildren() {
        throw new UnsupportedOperationException();
    }
}
