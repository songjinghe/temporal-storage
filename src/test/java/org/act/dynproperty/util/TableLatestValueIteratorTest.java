package org.act.dynproperty.util;

import org.act.dynproperty.impl.InternalKey;
import org.act.dynproperty.impl.MemTable;
import org.act.dynproperty.impl.ValueType;
import org.act.dynproperty.table.TableComparator;
import org.junit.Before;
import org.junit.Test;

public class TableLatestValueIteratorTest
{
    private MemTable table = new MemTable( TableComparator.instence() );
    private final int ID_NUM = 20;
    private final int PRO_NUM = 10;
    private final int TIME_NUM = 10;
    
    @Before
    public void setUp()
    {
        for( int t = 0; t<TIME_NUM; t++ )
            for( long i = 0; i<ID_NUM; i++ )
                for( int p = 0; p<PRO_NUM; p++ )
                {
                    Slice idSlice = new Slice( 12 );
                    idSlice.setLong( 0, i );
                    idSlice.setInt( 8, p );
                    InternalKey key = new InternalKey( idSlice, t, 20, ValueType.VALUE );
                    table.add( key.encode(), key.encode() );
                }
    }
    
    @Test
    public void test()
    {
        TableLatestValueIterator iterator = new TableLatestValueIterator( table.iterator() );
        while( iterator.hasNext() )
        {
            InternalKey key = new InternalKey( iterator.next().getKey() );
            System.out.println( key );
        }
    }
}
