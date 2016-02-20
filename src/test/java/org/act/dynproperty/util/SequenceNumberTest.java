package org.act.dynproperty.util;

import java.util.Random;

import junit.framework.Assert;

import org.act.dynproperty.impl.SequenceNumber;
import org.act.dynproperty.impl.ValueType;
import org.junit.Before;
import org.junit.Test;

import static org.act.dynproperty.impl.SequenceNumber.MAX_VALUE_LENGTH;

public class SequenceNumberTest
{
    
    private final int NUM = 10000;
    
    private int[] times = new int[NUM];
    private int[] valueLengths = new int[NUM];
    private ValueType[] valueTypes = new ValueType[NUM];
    private long[] sequences = new long[NUM];
    
    @Before
    public void setUp()
    {
        Random random = new Random( NUM );
        for( int i = 0; i<NUM; i++ )
        {
            times[i] = random.nextInt( Integer.MAX_VALUE );
            valueLengths[i] = random.nextInt( MAX_VALUE_LENGTH );
            if( times[i] % 3 == 0 )
                valueTypes[i] = ValueType.VALUE;
            else if( times[i] % 3 == 1 )
                valueTypes[i] = ValueType.DELETION;
            else
                valueTypes[i] = ValueType.INVALID;
            sequences[i] = SequenceNumber.packSequenceAndValueType( times[i], valueLengths[i], valueTypes[i] );
        }
    }
    
    @Test
    public void test()
    {
        for( int i = 0; i<NUM; i++ )
        {
            int time = SequenceNumber.unpackTime( sequences[i] );
            int valueLength = SequenceNumber.unpackValueLength( sequences[i] );
            ValueType type = SequenceNumber.unpackValueType( sequences[i] );
            Assert.assertEquals( times[i], time );
            Assert.assertEquals( valueLengths[i], valueLength );
            Assert.assertEquals( valueTypes[i].getPersistentId(), type.getPersistentId() );
        }
    }
}
