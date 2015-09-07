package org.act.graph.io;

import java.util.Arrays;

import org.act.graph.io.util.ValueConventor;
import org.hamcrest.core.Is;

public class DataRecord
{

    
    public final static int DATA_LENGTH = 8;
    public final static int DATA_RECORD_LEN = DATA_LENGTH + 8 + 8;
    
    private long starttime;
    private long endtime;
    private byte[] data = new byte[DATA_LENGTH];
    
    public DataRecord( long s, long e, byte[] data )
    {
        this.starttime = s;
        this.endtime = e;
        for( int i = 0; i<DATA_LENGTH; i++ )
            this.data[i] = data[i];
    }
    
    public DataRecord( long s, long e )
    {
        this.starttime = s;
        this.endtime = e;
    }
    
    public void setData( byte[] d )
    {
        for( int i = 0; i<DATA_LENGTH; i++ )
            this.data[i] = d[i];
    }
    
    public byte[] getData()
    {
        return this.data;
    }
    
    public long getStartTime()
    {
        return this.starttime;
    }
    
    public long getEndTime()
    {
        return this.endtime;
    }
    
    public byte[] getbytes()
    {
        byte[] toret = new byte[DATA_RECORD_LEN];
        int index = 0;
        
        //starttime
        byte[] time = ValueConventor.conventLong( starttime );
        for( byte b: time )
            toret[index++] = b;
        
        //endtime
        time = ValueConventor.conventLong( endtime );
        for( byte b: time )
            toret[index++] = b;
        
        //data
        for( byte b: data )
        {
            toret[index++] = b;
        }
        
        assert index == DATA_RECORD_LEN;
        return toret;
    }
    
    public boolean equals( Object other )
    {
        if( !(other instanceof DataRecord) )
            return false;
        DataRecord o = (DataRecord)other;
        boolean toret = this.starttime == o.starttime && this.endtime == o.endtime && Arrays.equals( this.data, o.data );
        return toret;
    }
}






