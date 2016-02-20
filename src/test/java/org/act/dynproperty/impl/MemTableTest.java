package org.act.dynproperty.impl;

import java.util.Map.Entry;

import junit.framework.Assert;

import org.act.dynproperty.impl.MemTable.MemTableIterator;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.util.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

public class MemTableTest
{ 
    private static MemTable table;
    private final static int TIME_NUMS = 100;
    private final static int PRO_NUMS = 50;
    private final static int ID_NUMS = 100;

    @BeforeClass
    public static void setUp()
    {
        table = new MemTable( TableComparator.instence() );
        for( long i = 0; i<ID_NUMS; i++ )
        {
            for( int p = 0; p<PRO_NUMS; p++ )
            {
                for( int t = 0; t<TIME_NUMS; t++ )
                {
                    Slice idSlice = new Slice( 12 );
                    idSlice.setLong( 0, i );
                    idSlice.setInt( 8, p );
                    Slice value = new Slice( 20 );
                    value.setLong( 0, i );
                    value.setInt( 8, p );
                    long sequence = SequenceNumber.packSequenceAndValueType( t, 20, ValueType.VALUE );
                    value.setLong( 12, sequence );
                    InternalKey key = new InternalKey( idSlice, t, 20, ValueType.VALUE );
                    table.add( key.encode(), value );
                }
            }
        }
    }
    
    
    @Test
    public void testChangeKey()
    {
        for( long i = 0; i<ID_NUMS; i++ )
        {
            for( int p = 0; p<PRO_NUMS; p++ )
            {
                for( int t = 0; t<TIME_NUMS; t++ )
                {
                    Slice idSlice = new Slice(12);
                    idSlice.setLong( 0, i );
                    idSlice.setInt( 8, p );
                    Slice key = new InternalKey( idSlice, t, 20, ValueType.DELETION ).encode();
                    Slice newvalue = new Slice( 20 );
                    newvalue.setLong( 0, i );
                    newvalue.setInt( 8, p );
                    long sequnence = SequenceNumber.packSequenceAndValueType( t, 20, ValueType.VALUE );
                    newvalue.setLong( 12, sequnence );
                    table.add( key, newvalue );
                }
            }
        }
        MemTableIterator iterator = table.iterator();
        for( long i = 0; i<ID_NUMS; i++ )
        {
            for( int p = 0; p<PRO_NUMS; p++ )
            {
                for( int t = 0; t<TIME_NUMS; t++ )
                {
                    Entry<Slice,Slice> entry = iterator.next();
                    InternalKey valuekey = new InternalKey( entry.getValue() );
                    InternalKey key = new InternalKey( entry.getKey() );
                    Slice idSlice = new Slice( 12 );
                    idSlice.setLong( 0, i );
                    idSlice.setInt( 8, p );
                    Assert.assertEquals( idSlice, valuekey.getId() );
                    Assert.assertEquals( ValueType.VALUE.getPersistentId(), valuekey.getValueType().getPersistentId() );
                    Assert.assertEquals( ValueType.DELETION.getPersistentId(), key.getValueType().getPersistentId() );
                }
            }
        }
    }
    
    @Test
    public void testChangeValue()
    {
        for( long i = 0; i<ID_NUMS; i++ )
        {
            for( int p = 0; p<PRO_NUMS; p++ )
            {
                for( int t = 0; t<TIME_NUMS; t++ )
                {
                    Slice idSlice = new Slice(12);
                    idSlice.setLong( 0, i );
                    idSlice.setInt( 8, p );
                    Slice key = new InternalKey( idSlice, t, 20, ValueType.VALUE ).encode();
                    Slice newvalue = new Slice( 20 );
                    newvalue.setLong( 0, i+1 );
                    newvalue.setInt( 8, p+1 );
                    long sequnence = SequenceNumber.packSequenceAndValueType( t, 20, ValueType.DELETION );
                    newvalue.setLong( 12, sequnence );
                    table.add( key, newvalue );
                }
            }
        }
        MemTableIterator iterator = table.iterator();
        for( long i = 0; i<ID_NUMS; i++ )
        {
            for( int p = 0; p<PRO_NUMS; p++ )
            {
                for( int t = 0; t<TIME_NUMS; t++ )
                {
                    Entry<Slice,Slice> entry = iterator.next();
                    InternalKey valuekey = new InternalKey( entry.getValue() );
                    InternalKey key = new InternalKey( entry.getKey() );
                    Slice idSlice = new Slice( 12 );
                    idSlice.setLong( 0, i+1 );
                    idSlice.setInt( 8, p+1 );
                    Assert.assertEquals( idSlice, valuekey.getId() );
                    Assert.assertEquals( ValueType.DELETION.getPersistentId(), valuekey.getValueType().getPersistentId() );
                    Assert.assertEquals( ValueType.VALUE.getPersistentId(), key.getValueType().getPersistentId() );
                }
            }
        }
    }
}
