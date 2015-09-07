package org.act.graph.io.util;

import junit.framework.Assert;

import org.junit.Test;

public class ValueConventorTest
{
    @Test
    public void testConventLong()
    {
        //0
        long expacted = 0;
        byte[] value = ValueConventor.conventLong( expacted );
        for ( int i = 0; i < value.length; i++ )
        {
            Assert.assertEquals( 0, value[i] );
        }
        
        //1
        expacted = 1;
        value = ValueConventor.conventLong( expacted );
        Assert.assertEquals( 1, value[7] );
        for ( int i = 6; i >= 0; i-- )
        {
            Assert.assertEquals( 0, value[i] );
        }
        
        //-1
        expacted = -1;
        value = ValueConventor.conventLong( expacted );
        for ( int i = 0; i < value.length; i++ )
        {
            Assert.assertEquals( -1, value[i] );
        }
        
        //0000-1000 0000-0111 0000-0110 0000-0101 0000-0100 0000-0011 0000-0010 0000-0001
        expacted = 578437695752307201L;
        value = ValueConventor.conventLong( expacted );
        for ( int i = 0; i < value.length; i++ )
        {
            Assert.assertEquals( 8-i, value[i] );
        }
    }
}
