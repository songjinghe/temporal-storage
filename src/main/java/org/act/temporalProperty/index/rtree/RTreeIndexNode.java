package org.act.temporalProperty.index.rtree;

import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by song on 2018-01-19.
 */
public class RTreeIndexNode extends RTreeNode {

    private List<RTreeNode> children = new ArrayList<>();

    public RTreeIndexNode(List<RTreeNode> children, IndexEntryOperator op) {
        this.children = children;
        this.updateBound(op);
    }

    public RTreeIndexNode(List<RTreeNode> data) {
        this.children = data;
    }

    @Override
    public List<RTreeNode> getChildren() {
        return children;
    }

    @Override
    public void encode(SliceOutput out) {
        List<RTreeNode> data = this.getChildren();
        out.writeInt(data.size());
        for (RTreeNode entry : data) {
            out.writeInt(entry.getPos());
            out.writeInt(entry.getBound().getMin().length());
            out.writeBytes(entry.getBound().getMin());
            out.writeInt(entry.getBound().getMax().length());
            out.writeBytes(entry.getBound().getMax());
        }
    }

    public static RTreeNode decode(SliceInput in, IndexEntryOperator op) {
        int count = in.readInt();
        List<RTreeNode> data = new ArrayList<>();
        for(int i=0; i<count; i++){
            int pos = in.readInt();
            int len = in.readInt();
            Slice min = in.readSlice(len);
            len = in.readInt();
            Slice max = in.readSlice(len);
            data.add(new RTreeDiskNode(pos, new RTreeRange(min, max, op)));
        }
        return new RTreeIndexNode(data);
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    private void updateBound(IndexEntryOperator op){
        if(this.getChildren().size()>0) {
            List<Slice> bounds = new ArrayList<>();
            for(RTreeNode node : this.getChildren()){
                bounds.add(node.getBound().getMin());
                bounds.add(node.getBound().getMax());
            }
            this.bound = new RTreeRange(bounds, op);
        }
    }

    @Override
    public List<Slice> getEntries() {
        throw new RuntimeException("SNH: unsupported method");
    }

}
