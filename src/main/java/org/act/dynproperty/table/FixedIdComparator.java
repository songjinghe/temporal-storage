package org.act.dynproperty.table;

import org.act.dynproperty.util.Slice;

import com.google.common.base.Preconditions;

/**
 * 对能够唯一标识动态属性Id的Slice进行比较的工具类
 *
 */
public class FixedIdComparator implements UserComparator
{

    @Override
    public int compare( Slice o1, Slice o2 )
    {
        Preconditions.checkArgument( o1.length() == 12,"FixedIdComparator only compare the fixed format of slice" );
        Preconditions.checkArgument( o2.length() == 12,"FixedIdComparator only compare the fixed format of slice" );
        Slice id1 = o1.copySlice( 0, 8 );
        Slice id2 = o2.copySlice( 0, 8 );
        int toret = id1.compareTo( id2 );
        if( toret != 0 )
            return toret;
        return o1.getInt( 8 ) - o2.getInt( 8 );
    }

    @Override
    public String name()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Slice findShortestSeparator( Slice start, Slice limit )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Slice findShortSuccessor( Slice key )
    {
        // TODO Auto-generated method stub
        return null;
    }
}
