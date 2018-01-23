package org.act.temporalProperty.index;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.index.rtree.IndexEntryOperator;
import org.act.temporalProperty.index.rtree.RTreeNode;
import org.act.temporalProperty.index.rtree.RTreeNodeBlock;
import org.act.temporalProperty.index.rtree.RTreeRange;
import org.act.temporalProperty.util.Slice;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by song on 2018-01-19.
 */
public class IndexTableIterator extends AbstractIterator<Slice> implements PeekingIterator<Slice> {

    private final IndexEntryOperator op;
    private final LinkedList<RTreeNode> nodeStack = new LinkedList<>();
    private final LinkedList<Integer> indexStack = new LinkedList<>(); // next index to access
    private final MappedByteBuffer map;
    private final int rootPos;
    private final RTreeRange rootBound;
    private final RTreeRange queryBound;

    public IndexTableIterator(FileChannel channel, IndexQueryRegion regions, IndexEntryOperator indexEntryOperator) throws IOException {
        this.map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        this.map.order(ByteOrder.LITTLE_ENDIAN);
//        this.map.flip();
        this.op = indexEntryOperator;

//        System.out.println(map.limit()+" "+map.position()+" "+map.remaining());
        this.rootPos = map.getInt();
//        System.out.println(rootPos);
        this.rootBound = this.readRootRange();
        this.queryBound = op.toRTreeRange(regions);

        this.nodeStack.push(getNode(rootPos, rootBound));
        this.indexStack.push(0);
    }

    private RTreeRange readRootRange() {
        int len = map.getInt();
        byte[] min = new byte[len];
        map.get(min);
        len = map.getInt();
        byte[] max = new byte[len];
        map.get(max);
        return new RTreeRange(new Slice(min), new Slice(max), op);
    }

    private RTreeNode getNode(int pos, RTreeRange bound) {
        map.position(pos);
        RTreeNodeBlock block = new RTreeNodeBlock(map, op);
        RTreeNode node = block.getNode();
        node.setBound(bound);
        return node;
    }

    @Override
    protected Slice computeNext() {
        while(nodeStack.peek()!=null){
            RTreeNode cur = nodeStack.peek();
            int curIndex = indexStack.peek();
            if(!cur.isLeaf()){
                List<RTreeNode> children=cur.getChildren();
                if(curIndex<children.size()) {
                    RTreeNode diskNode = children.get(curIndex);
                    if(diskNode.getBound().overlap(queryBound)) {
                        RTreeNode node = getNode(diskNode.getPos(), diskNode.getBound());
                        nodeStack.push(node);
                        indexStack.push(0);
                    }else{
                        incLast(indexStack);
                    }
                }else{
                    nodeStack.pop();
                    indexStack.pop();
                    if(!indexStack.isEmpty()) incLast(indexStack);
                }
            }else{
                List<Slice> data = cur.getEntries();
                if(curIndex<data.size()){
                    Slice entry = data.get(curIndex);
                    if(queryBound.contains(entry)) {
                        incLast(indexStack);
                        return entry;
                    }else{
                        incLast(indexStack);
                    }
                }else{
                    nodeStack.pop();
                    indexStack.pop();
                    if(!indexStack.isEmpty()) incLast(indexStack);
                }
            }
        }
        return endOfData();
    }

    private void incLast(LinkedList<Integer> indexStack) {
        int i = indexStack.pop();
        indexStack.push(i+1);
    }
}
