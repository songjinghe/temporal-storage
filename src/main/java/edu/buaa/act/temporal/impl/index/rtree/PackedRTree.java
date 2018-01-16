package edu.buaa.act.temporal.impl.index.rtree;

import edu.buaa.act.temporal.impl.index.IndexDataEntry;

import java.util.ArrayList;
import java.util.List;

abstract public class PackedRTree<C extends IndexDataEntry<C>> extends RTree<C>
{
    protected static int b = 4096/32;
    protected final CoordinateGen<C> coordinateGen;

    public PackedRTree(CoordinateGen<C> coordinateGen){
        this.coordinateGen = coordinateGen;
    }

    abstract protected void sort(List<RNode<C>> data);

    protected RNode<C> pack(List<RNode<C>> data)
    {
        this.sort(data);
        List<RNode<C>> upperLevelNodes = new ArrayList<>();
        for(int i=b; i<=data.size(); i+=b)
        {
            RNode<C> node = RNode.newInstance(coordinateGen);
            node.setChildren(data.subList(i-b, i));
            node.updateBound();
            upperLevelNodes.add(node);
        }
        if(data.size()%b!=0){
            int s = (data.size()/b)*b;
            RNode<C> node = RNode.newInstance(coordinateGen);
            node.setChildren(data.subList(s, data.size()));
            node.updateBound();
            upperLevelNodes.add(node);
        }
        RTree.log.info("one level packed, ({}) nodes", upperLevelNodes.size());
        if(upperLevelNodes.size()>1){
            return pack(upperLevelNodes);
        }else if(upperLevelNodes.size()==1){
            return upperLevelNodes.get(0);
        }else{
            throw new RuntimeException("should not happen");
        }
    }


}
