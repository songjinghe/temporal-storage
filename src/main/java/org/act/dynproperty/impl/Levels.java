package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.act.dynproperty.util.Slice;

public class Levels
{
    private Level0 level0;
    private Level1 level1;
    private int boundary;
    private String dbDir;
    
    public Levels( String dbDir  )
    { 
        this.dbDir = dbDir;
        this.level1 = new Level1( dbDir );
        restoreMetaInfo();
    };
    
    private void restoreMetaInfo()
    {
        File manifast = new File( dbDir + "/" + Filename.logFileName( 0 ) );
        if(!manifast.exists())
            return;
        try( FileChannel channel = new FileInputStream( manifast ).getChannel() )
        {
            LogReader reader = new LogReader( channel, null , false, 0 );
            List<FileMetaData> level0s = new ArrayList<FileMetaData>(5);
            for( int i = 0; i<5; i++ )
                level0s.add( null );
            List<FileMetaData> level1s = new LinkedList<FileMetaData>();
            for( Slice slice = reader.readRecord(); slice != null ; slice = reader.readRecord() )
            {
                VersionEdit edit = new VersionEdit( slice );
                for( Entry<Integer,FileMetaData> entry : edit.getNewFiles().entries() )
                {
                    if( entry.getKey() == 1 )
                        level1s.add( entry.getValue() );
                    else
                    {
                        level0s.set( (int)(entry.getValue().getNumber()), entry.getValue() );
                    }
                }
                for( Entry<Integer,Long> entry : edit.getDeletedFiles().entries() )
                {
                    level0s.set( entry.getValue().intValue(), null );
                }
            }
            this.level1.setFiles( level1s );
            int index = -1;
            for( FileMetaData meta : level0s )
            {
                if( meta != null )
                {
                    index++;
                }
            }
            if( index > -1 )
            {
                this.level0 = new Level0( dbDir, this.level1 );
                this.level0.setFiles( level0s );
                this.level0.setStart(level0s.get( index ).getSmallest());
                this.boundary = this.level0.getStart();
            }
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void add( long id, int proId, int startTime, Slice value )
    {
        if( null == level0 )
        {
            level0 = new Level0( dbDir, level1 );
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
        //FIXME
        if( -1 == time )
            time = Integer.MAX_VALUE;
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        if( time >= boundary )
            return level0.getPointValue( idSlice, time );
        else
            return level1.getPointValue( idSlice, time );
    }
    
    public Slice getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        //FIXME
        if( -1 == endTime )
            endTime = Integer.MAX_VALUE;
        if( startTime>= boundary )
            return level0.getRangeValue( id, proId, startTime, endTime, callback );
        else if( endTime < boundary )
            return level1.getRangeValue( id, proId, startTime, endTime, callback );
        else
        {
            level1.getRangeValue( id, proId, startTime, boundary, callback );
            level0.getRangeValue( id, proId, boundary, endTime, callback );
            return callback.onReturn();
        }
    }

    public int getPropertyLatestTime( long Id, int propertyKeyId )
    {
        int toret = level0.getPropertyLatestTime( Id, propertyKeyId );
        if( -1 == toret )
            toret = level1.getPropertyLatestTime( Id, propertyKeyId );
        return toret;
    }
}
