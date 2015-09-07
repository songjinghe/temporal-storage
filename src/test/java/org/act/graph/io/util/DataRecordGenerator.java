package org.act.graph.io.util;

import org.act.graph.io.DataRecord;

public class DataRecordGenerator
{
    private static long startTimeSeed = 1;
    private static long entTimeSeed = startTimeSeed + 1;
    
    public static DataRecord generatDataRecord()
    {
        DataRecord toret = new DataRecord( startTimeSeed, entTimeSeed );
        byte[] data = ValueConventor.conventLong( 1152921504606846975L );
        toret.setData( data );
        startTimeSeed += 2;
        entTimeSeed += 2;
        return toret;
    }
    
}
