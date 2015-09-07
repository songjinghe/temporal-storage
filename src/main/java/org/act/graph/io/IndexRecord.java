package org.act.graph.io;

public class IndexRecord
{
    public final static int RECORD_SIZE = 24;
    
    private long starttime;
    private long endtime;
    private long data_block_offset;
    
    public IndexRecord( long s, long e, long d )
    {
        this.starttime = s;
        this.endtime = e;
        this.data_block_offset = d;
    }
    
    public long getstart()
    {
        return starttime;
    }
    
    public long getend()
    {
        return endtime;
    }
    
    public long getdataoffset()
    {
        return data_block_offset;
    }
}
