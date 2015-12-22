package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.act.dynproperty.table.FileChannelTable;
import org.act.dynproperty.table.Table;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.util.Slice;
import org.act.dynproperty.util.TableIterator;

import com.google.common.base.Preconditions;

public class Level1
{
    private List<FileMetaData> files = new ArrayList<FileMetaData>();
    
    private String dbDir;
    private int start;
    private int end;
    
    private static Level1 instence;
    private Level1( String db)
    {
        this.dbDir = db;
    }
  
    public static synchronized Level1 instence( String db)
    {
        if(null==instence)
            instence = new Level1(db);
        return instence;
    }
    
    public Slice getPointValue(Slice id , int time )
    {
        FileMetaData target = null;
        for( FileMetaData meta : this.files )
        {
            if( time >= meta.getSmallest() && time < meta.getLargest() )
            {
                target = meta;
                break;
            }
        }
        if( null == target )
            return null;
        String filename = Filename.stableFileName( target.getNumber() );
        try
        {
            FileChannel channel = new FileInputStream( new File( this.dbDir + "/" + filename ) ).getChannel();
            Table table = new FileChannelTable( filename, channel, TableComparator.instence(), false );
            TableIterator iterator = table.iterator();
            iterator.seek( new InternalKey( id, time, ValueType.VALUE).encode() );
            Entry<Slice,Slice> entry = iterator.next();
            Slice key = entry.getKey();
            Slice rid = key.copySlice( 0, 12 );
            Preconditions.checkArgument( id.equals( rid ), "Get value faild because returned id is wrong");
            return entry.getValue();
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
    public void addFile( FileMetaData fileMeta )
    {
        files.add( fileMeta );
        end = fileMeta.getLargest();
        start = Math.min( start, fileMeta.getSmallest() );
    }
    
    public int getNextFileNum()
    {
        return this.files.size();
    }

    public Slice getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        for( int i = 0; i <this.files.size(); i++ )
        {
            FileMetaData metaData = this.files.get( i );
            if( null != metaData )
            {
                if( startTime <= metaData.getSmallest() && endTime >= metaData.getLargest() )
                {
                    int start = Math.max( startTime, metaData.getSmallest() );
                    int end = Math.min( endTime, metaData.getLargest() );
                    String fileName = this.dbDir + "/" + Filename.unStableFileName( metaData.getNumber() );
                    try( FileChannel channel = new FileInputStream( new File(fileName) ).getChannel() )
                    {
                        TableIterator iterator = new FileChannelTable( Filename.unStableFileName( metaData.getNumber() ), 
                                channel, TableComparator.instence(), false ).iterator();
                        iterator.seek( new InternalKey( idSlice, start, ValueType.VALUE ).encode() );
                        while( iterator.hasNext() )
                        {
                            Entry<Slice,Slice> entry = iterator.next();
                            InternalKey key = new InternalKey( entry.getKey() );
                            Preconditions.checkArgument( key.getId().equals( idSlice ), "Get value faild because returned id is wrong in range query of level0" );
                            if( key.getStartTime() <= end )
                                callback.onCall( entry.getValue() );
                            else
                                break;
                        }
                    }
                    catch ( IOException e )
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }
        return callback.onReturn();
    }
}
