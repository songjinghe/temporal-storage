package org.act.dynproperty.util;

import org.junit.Test;

public class SliceCompareTest
{
    @Test
    public void compareTest()
    {
        Slice slice1 = new Slice( 8 );
        Slice slice2 = new Slice( 8 );
        slice1.setLong( 0, 5435413 );
        slice2.setLong( 0, 4134354 );
        System.out.println(slice1.compareTo( slice2 ));
    }
}
