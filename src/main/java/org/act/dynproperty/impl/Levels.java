package org.act.dynproperty.impl;

import org.act.dynproperty.util.Slice;

public class Levels
{
    private Level0 level0;
    private Level1 level1;
    private int boundary;
    private String dbDir;
    
    private static Levels instence;
    private Levels( String dbDir  )
    { 
        this.dbDir = dbDir;
        this.level1 = Level1.instence( dbDir );
    };
    
    public static synchronized Levels instence( String db )
    {
        if( instence == null )
            instence = new Levels( db );
        return instence;
    }
    
    public void add( long id, int proId, int startTime, Slice value )
    {
        if( null == level0 )
        {
            level0 = Level0.newInstence( this.dbDir, startTime );
        }
        Slice idSlice = new Slice( 12 );
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        level0.add( idSlice, ValueType.VALUE, startTime, value );
        if( level0.getStart() > boundary )
            boundary = level0.getStart();
    }
    
    public Slice getPointValue( long id, int proId, int time )
    {
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        if( time >= boundary )
            return level0.getPointValue( idSlice, time );
        else
            return level1.getPointValue( idSlice, time );
    }
}
