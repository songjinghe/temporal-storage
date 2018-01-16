package edu.buaa.act.temporal.impl.index.rtree;

import edu.buaa.act.temporal.impl.index.IndexDataEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RNode<C extends IndexDataEntry<C>>
{
    private final RTree.CoordinateGen<C> cGen;
    private C center;
    private CoordinateRange<C> bound;
    private boolean isLeaf = false;
    private List<RNode<C>> children = new ArrayList<>();

    public RNode(RTree.CoordinateGen<C> coordinateGen, C d){
        this.cGen = coordinateGen;
        this.center = d;
        this.isLeaf = true;
    }

    public RNode(RTree.CoordinateGen<C> coordinateGen){
        this.cGen = coordinateGen;
        this.center = cGen.newInstance();
        this.isLeaf = false;
    }

    public List<RNode<C>> getChildren(){
        return children;
    }

    public void setChildren(List<RNode<C>> children) {
        this.children = children;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public void updateBound(){
        if(!this.isLeaf() && this.getChildren().size()>0) {
            C minBound = this.cGen.newInstance(this.getChildren().get(0).getCenter());
            C maxBound = this.cGen.newInstance(this.getChildren().get(0).getCenter());
            for(RNode<C> node : this.getChildren()){
                if(node.isLeaf()){
                    minBound.updateMin(node.getCenter());
                    maxBound.updateMax(node.getCenter());
                }else {
                    minBound.updateMin(node.getBound().getMin());
                    maxBound.updateMax(node.getBound().getMax());
                }
            }
            C center = this.getCenter();
            center.setToAvg( minBound, maxBound );
            this.bound = new CoordinateRange<>(minBound, maxBound);
        }
    }

    public C getCenter() {
        return center;
    }

    public CoordinateRange<C> getBound() {
        return bound;
    }

    public CoordinateRange getBound(RNode<C> nodes) {

        return bound;
    }

    public List<RNode<C>> query(CoordinateRange<C> range)
    {
        if(!isLeaf()){
            if(range.overlap(this.bound)){
                RTree.nodeAccessCount++;
                List<RNode<C>> result = new ArrayList<>();
                for(RNode<C> child : children){
                    result.addAll(child.query(range));
                }
                return result;
            }else{
                return Collections.emptyList();
            }
        }else{
            if(range.contains(this.center)){
                RTree.nodeAccessCount++;
                List<RNode<C>> result = new ArrayList<>();
                result.add(this);
                return result;
            }else{
                return Collections.emptyList();
            }
        }
    }

    public static <COR extends IndexDataEntry<COR>> Comparator<RNode<COR>> getComparator(COR cor, int dimIndex){
        return (o1, o2) -> o1.center.compareTo(o2.center, dimIndex);
    }

    public static <C extends IndexDataEntry<C>> RNode<C> newInstance(RTree.CoordinateGen<C> coordinateGen) {
        return new RNode<>(coordinateGen);
    }

    public void setCenter(C center) {
        this.center = center;
    }
}
