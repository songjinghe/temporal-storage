package org.act.temporalProperty.util;

public class TimeIntervalUtil
{
    public static boolean overlap(int start1, int end1, int start2, int end2 )
    {
        return !( end1 < start2 || end2 < start1 );
    }

    public static boolean contains(int start1, int end1, int start2, int end2 )
    {
        return start1<=start2 && end1>=end2;
    }
}
