package org.act.graph.io.util;

public class ValueConventor
{
    public static byte[] conventLong( long num )
    {
        byte[] toret = new byte[8];
        byte temp;
        for( int i = 7; i >=0; i-- )
        {
            temp = (byte)(num >> i * 8);
            toret[7-i] = temp;
        }
        return toret;
    }
}
