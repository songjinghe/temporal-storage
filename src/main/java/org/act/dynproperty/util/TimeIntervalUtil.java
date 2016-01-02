package org.act.dynproperty.util;

public class TimeIntervalUtil
{
    public static boolean Union( int start1, int end1, int start2, int end2 )
    {
        return !( end1 < start2 || end2 < start1 );
    }
}
