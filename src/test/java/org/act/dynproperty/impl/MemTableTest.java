package org.act.dynproperty.impl;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.act.dynproperty.table.MemTableComparator;
import org.act.dynproperty.util.Slice;
import org.junit.Before;
import org.junit.Test;

/**
 * 测试的数据为多个id，每个id有多个时间，时间以10为间隔。
 *
 *
 * @author huanghx( huanghx@act.buaa.edu.cn )
 */
public class MemTableTest
{ 
    private List<Long> ids = new LinkedList<Long>();
    private List<Integer> times = new LinkedList<Integer>();
    private MemTable table;
    private final int NUM = 10;

    @Before
    public void setUp()
    {
        for( long i = 0; i < NUM; i++ )
        {
            ids.add( i );
            times.add( (int)i * 10 );
        }
        table = new MemTable( MemTableComparator.instence(), 0 );
        for( int i = 0; i<ids.size(); i++ )
        {
            for( int t = 0; t<times.size(); t++ )
            {
                long id = ids.get( i );
                int time = times.get( t );
                Slice idSlice = new Slice( 12 );
                idSlice.setLong( 0, id );
//                byte[] bytes = idSlice.getBytes();
//                for( int j = 0; j<8; j++ )
//                    bytes[j] += (byte)'0';
//                idSlice.setBytes( 0, bytes, 0, 8 );
                Slice valueSlice = new Slice( 4 );
                valueSlice.setInt( 0, time );
                table.add( idSlice, ValueType.VALUE, time, valueSlice );
            }
        }
    }
    
    @Test
    public void testValue()
    {
        for( int i = 0; i< NUM; i++ )
        {
            for( int t = 0; t<times.size(); t++ )
            {
                long id = ids.get( i );
                int time = times.get( t );
                Slice idSlice = new Slice( 12 );
                idSlice.setLong( 0, id );
//                byte[] bytes = idSlice.getBytes();
//                for( int j = 0; j<8; j++ )
//                    bytes[j] += (byte)'0';
//                idSlice.setBytes( 0, bytes, 0, 8 );
                LookupKey key = new LookupKey( idSlice, time );
                LookupResult result = table.get( key );
                Assert.assertEquals( (int)times.get( t ), result.getValue().getInt( 0 ) );
            }
        }
    }
}
