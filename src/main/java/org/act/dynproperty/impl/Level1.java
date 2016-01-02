package org.act.dynproperty.impl;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.util.InternalTableIterator;
import org.act.dynproperty.util.Slice;
import org.act.dynproperty.util.TimeIntervalUtil;

public class Level1
{
    private List<FileMetaData> files = new LinkedList<FileMetaData>();
    private TableCache tableCache;
    private int start;
    public Level1( String db)
    {
        File dbFile = new File(db);
        tableCache = new TableCache( dbFile, 10, TableComparator.instence(), false, true );
    }
  
    
    public ReturnValue getPointValue(Slice id , int time )
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
//        String filename = Filename.stableFileName( target.getNumber() );
//        try
//        {
//            FileChannel channel = new FileInputStream( new File( this.dbDir + "/" + filename ) ).getChannel();
//            Table table = new FileChannelTable( filename, channel, TableComparator.instence(), false );
//            TableIterator iterator = table.iterator();
//            iterator.seek( new InternalKey( id, time, ValueType.VALUE).encode() );
//            Entry<Slice,Slice> entry = iterator.next();
//            Slice key = entry.getKey();
//            Slice rid = key.copySlice( 0, 12 );
//            Preconditions.checkArgument( id.equals( rid ), "Get value faild because returned id is wrong");
//            return entry.getValue();
//        }
//        catch ( IOException e )
//        {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        return null;
        InternalTableIterator iterator = tableCache.newIterator( target );
        iterator.seekInternal( new InternalKey( id, time, ValueType.VALUE ));
        Entry<InternalKey,Slice> entry = iterator.next();
        InternalKey key = entry.getKey();
        Slice rid = key.getId();
        if( !rid.equals( id ) )
        {
            //FIXME query the current file
            return new ReturnValue( false );
        }
        else
            return new ReturnValue( entry.getValue() );
    }
    
    public void addFile( FileMetaData fileMeta )
    {
        files.add( fileMeta );
        fileMeta.getLargest();
        start = Math.min( start, fileMeta.getSmallest() );
    }
    
    public int getNextFileNum()
    {
        return this.files.size();
    }

    public void getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        for( int i = 0; i <this.files.size(); i++ )
        {
            FileMetaData metaData = this.files.get( i );
            if( null != metaData )
            {
                if( TimeIntervalUtil.Union( startTime, endTime, metaData.getSmallest(), metaData.getLargest() ) )
                {
                    int start = Math.max( startTime, metaData.getSmallest() );
                    int end = Math.min( endTime, metaData.getLargest() );
//                    String fileName = this.dbDir + "/" + Filename.unStableFileName( metaData.getNumber() );
//                    try( FileChannel channel = new FileInputStream( new File(fileName) ).getChannel() )
//                    {
//                        TableIterator iterator = new FileChannelTable( Filename.unStableFileName( metaData.getNumber() ), 
//                                channel, TableComparator.instence(), false ).iterator();
//                        iterator.seek( new InternalKey( idSlice, start, ValueType.VALUE ).encode() );
//                        while( iterator.hasNext() )
//                        {
//                            Entry<Slice,Slice> entry = iterator.next();
//                            InternalKey key = new InternalKey( entry.getKey() );
//                            Preconditions.checkArgument( key.getId().equals( idSlice ), "Get value faild because returned id is wrong in range query of level0" );
//                            if( key.getStartTime() <= end )
//                                callback.onCall( entry.getValue() );
//                            else
//                                break;
//                        }
//                    }
//                    catch ( IOException e )
//                    {
//                        // TODO Auto-generated catch block
//                        e.printStackTrace();
//                    }
                    InternalTableIterator iterator = this.tableCache.newIterator( metaData );
                    iterator.seek( new InternalKey( idSlice, start, ValueType.VALUE ));
                    while( iterator.hasNext() )
                    {
                        Entry<InternalKey,Slice> entry = iterator.next();
                        InternalKey key = entry.getKey();
                        if( !idSlice.equals( key.getId() ) )
                            break;
                        if( key.getValueType().getPersistentId() == ValueType.DELETION.getPersistentId() )
                            return;
                        if( key.getStartTime() <= end )
                            callback.onCall( entry.getValue() );
                        else
                            break;
                    }
                }
            }
        }
        return;
    }


    public int getPropertyLatestTime( long id, int propertyKeyId )
    {
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, propertyKeyId );
        InternalKey searchKey = new InternalKey( idSlice, Integer.MAX_VALUE, ValueType.VALUE );
        for( int i = 0; i<5; i++ )
        {
            FileMetaData metaData = this.files.get( i );
            if( null == metaData )
            {
                InternalTableIterator tableIterator = this.tableCache.newIterator( metaData );
                tableIterator.seek( searchKey );
                if( tableIterator.hasNext() )
                {
                    InternalKey targetKey = tableIterator.next().getKey();
                    if( targetKey.getId().equals( searchKey.getId() ) )
                        return (int)targetKey.getStartTime();
                }
            }
        }
        return -1;
    }


    public void setFiles( List<FileMetaData> level1s )
    {
        this.files = new LinkedList<FileMetaData>( level1s );
    }
}
