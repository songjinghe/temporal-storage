package edu.buaa.act.temporal.impl.index.rtree;

import edu.buaa.act.temporal.impl.index.IndexDataEntry;

import java.util.List;

/**
 * Created by song on 2018-01-10.
 */
public class STRTree<C extends IndexDataEntry<C>> extends PackedRTree<C>
{

    public STRTree(List<RNode<C>> list, CoordinateGen<C> coordinateGen)
    {
        super(coordinateGen);
        this.setRoot(this.pack(list));
    }

    @Override
    protected void sort(List<RNode<C>> data)
    {
        int k = this.coordinateGen.newInstance().dimCount();
        recursiveSort(data, 0, data.size(), 0, k);
    }

    private void recursiveSort(List<RNode<C>> data, int left, int right, int corIndex, int k)
    {
        data.subList(left, right).sort(RNode.getComparator(this.coordinateGen.newInstance(), corIndex));
        if (k > 1)
        {
            int r = right - left;
            int p = r / b + (r % b == 0 ? 0 : 1);
            int s = (int) Math.round(Math.ceil(Math.pow(p, 1d / k)));
            int groupLen = s * b;
            for (int i = 0; i < s; i++)
            {
                int start = i * groupLen;
                int end = i * (groupLen + 1) > right ? right : i * (groupLen + 1);
                recursiveSort(data, start, end, corIndex + 1, k - 1);
            }
        }
    }


}
