package org.act.temporalProperty.index.rtree;

import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceOutput;

import java.util.List;

/**
 * Created by song on 2018-01-19.
 *
 */
public abstract class RTreeNode
{
    protected int pos;
    protected RTreeRange bound;

    // common methods ========================
    public abstract boolean isLeaf();
    public abstract void encode(SliceOutput out);

    public void setPos(long position) {
        if(position<Integer.MAX_VALUE) this.pos = (int) position;
        else throw new RuntimeException("pos large than 2GB");
    }

    public int getPos() {
        return pos;
    }

    public RTreeRange getBound() {
        return bound;
    }

    public void setBound(RTreeRange range) {
        this.bound = range;
    }

    // method only for index nodes ================
    public abstract List<RTreeNode> getChildren();

    // method only for leaf nodes ================
    public abstract List<Slice> getEntries();
}
