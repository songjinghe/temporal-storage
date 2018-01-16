package edu.buaa.act.temporal.impl.index.rtree;

import edu.buaa.act.temporal.impl.index.IndexDataEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RTree<C extends IndexDataEntry<C>>
{
    public static Logger log = LoggerFactory.getLogger("test");
    public static long nodeAccessCount = 0;

    private RNode<C> root;

    public List<RNode<C>> query(CoordinateRange<C> range){
        return root.query(range);
    }

    public void setRoot(RNode<C> root) {
        this.root = root;
    }

    public static abstract class CoordinateGen<T extends IndexDataEntry<T>>{
        abstract public T newInstance(T center);
        abstract public T newInstance();
    }
}
