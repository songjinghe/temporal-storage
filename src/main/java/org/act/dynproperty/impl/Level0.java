package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.act.dynproperty.impl.MemTable.MemTableIterator;
import org.act.dynproperty.table.FileChannelTable;
import org.act.dynproperty.table.MemTableComparator;
import org.act.dynproperty.table.Table;
import org.act.dynproperty.table.TableBuilder;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.util.InternalIterator;
import org.act.dynproperty.util.InternalTableIterator;
import org.act.dynproperty.util.MergingIterator;
import org.act.dynproperty.util.Slice;
import org.act.dynproperty.util.TimeIntervalUtil;

public class Level0
{
    private List<FileMetaData> files = new ArrayList<FileMetaData>();
    private TableCache tableCache;
    private MemTable memTable;
    private MemTable stableTable;
    private static Lock lock = new ReentrantLock();
    private String dbDir;
    
    private int lastAddTime = 0;
    private int start;
    
    private Level1 level1;
    
    public Level0( String dbDir, Level1 level1 )
    {
        this.dbDir = dbDir;
        for(int i =0; i<5; i++ )
            files.add( null );
        File dbFile = new File( dbDir );
        this.tableCache = new TableCache( dbFile, 5, TableComparator.instence(), false, false );
        this.level1 = level1;
    }

    public ReturnValue getPointValue( Slice id, int time )
    {
        lock.lock();
        if( time >= this.memTable.getStartTime() )
        {
            LookupKey lookupKey = new LookupKey( id, time );
            lock.unlock();
            LookupResult result = this.memTable.get( lookupKey );
            if( !result.getKey().getId().equals( id ) )
                return new ReturnValue( false );
            else
                return new ReturnValue( result.getValue() );
        }
        else
        {
            FileMetaData target = null;
            for( FileMetaData meta : this.files )
            {
                if( null != meta )
                {
                    if( time >= meta.getSmallest()&& time < meta.getLargest() )
                    {
                        target = meta;
                        break;
                    }
                }
            }
//            String fileName = Filename.unStableFileName( target.getNumber() );
//            try( FileChannel channel = new FileInputStream( new File( this.dbDir + "/" + fileName ) ).getChannel() )
//            {
//                //FileChannel channel = new FileInputStream( new File( this.dbDir + "/" + fileName ) ).getChannel();
//                Table table = new FileChannelTable( fileName, channel, TableComparator.instence(), false );
//                TableIterator iterator = table.iterator();
//                iterator.seek( new InternalKey( id, time, ValueType.VALUE ).encode() );
//                Entry<Slice,Slice> entry = iterator.next();
//                Slice key = entry.getKey();
//                lock.unlock();
//                Slice rid = key.copySlice( 0, 12 );
//                Preconditions.checkArgument( id.equals( rid ), "Get value faild because returned id is wrong in point query of level0");
//                return entry.getValue();
//            }
//            catch ( IOException e )
//            {
//                e.printStackTrace();
//            }
            InternalTableIterator iterator = tableCache.newIterator( target );
            iterator.seekInternal( new InternalKey( id, time, ValueType.VALUE ));
            Entry<InternalKey,Slice> entry = iterator.next();
            lock.unlock();
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
    }
    
    public int getStart()
    {
        return this.start;
    }
    
    public void add( Slice id,  ValueType valueType, int startTime, Slice value )
    {
        if( null == this.memTable )
            this.memTable = new MemTable( MemTableComparator.instence(), startTime );
        if( this.memTable.approximateMemoryUsage() >= 4*1024*1024 && lastAddTime < startTime )
        {
            lock.lock();
            newMemTable( startTime );
            this.memTable.add( id, valueType, startTime, value );
            boolean noFiles = true;
            for( FileMetaData meta : this.files )
            {
                if( null != meta )
                {
                    noFiles = false;
                    break;
                }
            }
            if( noFiles )
            {
                this.start = startTime;
            }
            lastAddTime = startTime;
            lock.unlock();
        }
        else
        {
            this.memTable.add( id, valueType, startTime, value );
        }
        lastAddTime = startTime;
    }
    
    private void newMemTable( int newStartTime )
    {
        stableTable = memTable;
        memTable = new MemTable( MemTableComparator.instence() , newStartTime );
        dump2disc( stableTable, newStartTime );
    }
    
    private void dump2disc( MemTable table, int endTime )
    {
        if( files.get( 0 ) != null )
        {
            fileMerge( table, endTime );
        }
        else
        {
            try
            {
                String fileName = Filename.unStableFileName( 0 );
                File file = new File( this.dbDir + "/" + fileName );
                file.createNewFile();
                FileOutputStream stream = new FileOutputStream( file );
                TableBuilder tableBuilder = new TableBuilder( new Options(), stream.getChannel(), TableComparator.instence() );
                for( Entry<InternalKey,Slice> entry : this.stableTable )
                {
                    tableBuilder.add( entry.getKey().encode(), entry.getValue() );
                }
                tableBuilder.finish();
                FileMetaData newMetaData = new FileMetaData( 0, file.length(), table.getStartTime(), endTime );
                this.files.set( 0, newMetaData );
                stream.close();
                VersionEdit versionEdit = new VersionEdit();
                versionEdit.setComparatorName( "TableComparator" );
                versionEdit.setLastSequenceNumber( 0 );
                versionEdit.setLogNumber( 0 );
                versionEdit.setNextFileNumber( this.level1.getNextFileNum() );
                versionEdit.setPreviousLogNumber( 0 );
                versionEdit.addFile( 0, newMetaData );
                LogWriter write = Logs.createLogWriter( dbDir );
                write.addRecord( versionEdit.encode(), true );
                //write.close();
                
            }
            catch( IOException e)
            {
                //FIXME
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings( "resource" )
    private void fileMerge( MemTable table, int endTime )
    {
        int level = 1;
        while( level < 5 && this.files.get( level ) != null){ level++; };
        if( 5 == level )
            merge2Stable( table, endTime );
        else
        {
            List<InternalIterator> files2merge = new ArrayList<InternalIterator>(level+1);
            List<FileInputStream> stream2close = new ArrayList<FileInputStream>(level);
            files2merge.add( table.iterator() );
            int startTime = (int)this.files.get( level-1 ).getSmallest();
            for( int i = 0; i<level; i++ )
            {
                FileMetaData meta = this.files.get( i );
                File file = new File( this.dbDir + "/" + Filename.unStableFileName( meta.getNumber() ) );
                try
                {
                    FileInputStream stream = new FileInputStream( file );
                    stream2close.add( stream );
                    Table tablefile = new FileChannelTable( file.getAbsolutePath(), stream.getChannel(), TableComparator.instence(), false );
                    files2merge.add( new InternalTableIterator( tablefile.iterator() ) );
                }
                catch ( IOException e )
                {
                    //FIXME
                    e.printStackTrace();
                }
            }
            MergingIterator mergingIterator = new MergingIterator( files2merge, MemTableComparator.instence() );
            try
            {
                File result = new File( this.dbDir + "/" + Filename.unStableFileName( level ) );
                if( !result.exists() )
                    result.createNewFile();
                FileOutputStream stream = new FileOutputStream( result );
                TableBuilder builder = new TableBuilder( new Options(), stream.getChannel(), TableComparator.instence() );
                while( mergingIterator.hasNext() )
                {
                    Entry<InternalKey,Slice> entry = mergingIterator.next();
                    builder.add( entry.getKey().encode(), entry.getValue() );
                }
                builder.finish();
                stream.close();
                //FIXME
                FileMetaData metaData = new FileMetaData( level, result.length(), startTime, endTime );
                VersionEdit versionEdit = new VersionEdit();
                versionEdit.setComparatorName( "TableComparator" );
                versionEdit.setLastSequenceNumber( 0 );
                versionEdit.setLogNumber( 0 );
                versionEdit.setNextFileNumber( this.level1.getNextFileNum() );
                versionEdit.setPreviousLogNumber( 0 );
                this.files.set( level, metaData );
                versionEdit.addFile( 0, metaData );
                for( int i = 0; i<level; i++ )
                {
                    File delete = new File(this.dbDir + "/" + Filename.unStableFileName( i ));
                    stream2close.get( i ).close();
                    delete.delete();
                    versionEdit.deleteFile( 0, this.files.get( i ).getNumber() );
                    this.files.set( i, null );
                }
                LogWriter write = Logs.createLogWriter( dbDir );
                write.addRecord( versionEdit.encode(), true );
                //write.close();
            }
            catch( IOException e )
            {
                //FIXME
                e.printStackTrace();
            }
        }
    }

    private void merge2Stable( MemTable table, int endTime )
    {
        List<InternalIterator> files2merge = new ArrayList<InternalIterator>(6);
        List<FileInputStream> stream2delete = new ArrayList<FileInputStream>(5); 
        files2merge.add( table.iterator() );
        int startTime = (int)this.files.get( 4 ).getSmallest();
        for( int i = 0; i<5; i++ )
        {
            FileMetaData meta = this.files.get( i );
            File file = new File( this.dbDir + "/" + Filename.unStableFileName( meta.getNumber() ) );
            try
            {
                FileInputStream stream = new FileInputStream( file );
                stream2delete.add( stream );
                Table tablefile = new FileChannelTable( file.getAbsolutePath(), stream.getChannel(), TableComparator.instence(), false );
                files2merge.add( new InternalTableIterator( tablefile.iterator() ) );
            }
            catch ( IOException e )
            {
                //FIXME
                e.printStackTrace();
            }
        }
        MergingIterator mergingIterator = new MergingIterator( files2merge, MemTableComparator.instence() );
        try
        {
            long fileNum = this.level1.getNextFileNum();
            File result = new File( this.dbDir + "/" + Filename.stableFileName( fileNum ) );
            if( !result.exists() )
                result.createNewFile();
            FileOutputStream stream = new FileOutputStream( result );
            TableBuilder builder = new TableBuilder( new Options(), stream.getChannel(), TableComparator.instence() );
            while( mergingIterator.hasNext() )
            {
                Entry<InternalKey,Slice> entry = mergingIterator.next();
                builder.add( entry.getKey().encode(), entry.getValue() );
            }
            builder.finish();
            stream.close();
            //FIXME
            FileMetaData metaData = new FileMetaData( fileNum, result.length(), startTime, endTime );
            VersionEdit versionEdit = new VersionEdit();
            versionEdit.setComparatorName( "TableComparator" );
            versionEdit.setLastSequenceNumber( 0 );
            versionEdit.setLogNumber( 0 );
            versionEdit.setNextFileNumber( this.level1.getNextFileNum() );
            versionEdit.setPreviousLogNumber( 0 );
            for( int i = 0; i<5; i++ )
            {
                File delete = new File(this.dbDir + "/" + Filename.unStableFileName( i ));
                stream2delete.get( i ).close();
                delete.delete();
                versionEdit.deleteFile( 0, this.files.get( i ).getNumber() );
                this.files.set( i, null );
            }
            this.level1.addFile( metaData );
            versionEdit.addFile( 1, metaData );
            LogWriter write = Logs.createLogWriter( dbDir );
            write.addRecord( versionEdit.encode(), true );
            //write.close();
        }
        catch( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }

    public void getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        for( int i = 4; i >= 0; i-- )
        {
            FileMetaData metaData = this.files.get( i );
            if( null != metaData )
            {
                if( TimeIntervalUtil.Union( startTime, endTime, metaData.getSmallest(), metaData.getLargest() ) )
                {
                    int start = Math.max( startTime, (int)metaData.getSmallest() );
                    int end = Math.min( endTime,(int)metaData.getLargest() );
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
//                        e.printStackTrace();
//                    }
                    InternalTableIterator iterator = this.tableCache.newIterator( metaData );
                    iterator.seek( new InternalKey( idSlice, start, ValueType.VALUE ) );
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
        if( endTime >= this.memTable.getStartTime() )
        {
            MemTableIterator iterator = this.memTable.iterator();
            InternalKey startkey = new InternalKey( idSlice, this.memTable.getStartTime(), ValueType.VALUE );
            iterator.seek( startkey );
            while( iterator.hasNext() )
            {
                Entry<InternalKey,Slice> entry = iterator.next();
                if( !idSlice.equals( entry.getKey().getId() ) )
                    break;
                if( startkey.getValueType().getPersistentId() == ValueType.DELETION.getPersistentId() )
                    return;
                if( entry.getKey().getStartTime() <= endTime )
                    callback.onCall( entry.getValue() );
                else
                    break;
            }
        }
        return ;
    }

    public int getPropertyLatestTime( long id, int propertyKeyId )
    {
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, propertyKeyId );
        InternalKey searchKey = new InternalKey( idSlice, Integer.MAX_VALUE, ValueType.VALUE );
        MemTableIterator iterator = this.memTable.iterator();
        iterator.seek( searchKey );
        if( iterator.hasNext() )
        {
            InternalEntry targetKey = iterator.next();
            if( targetKey.getKey().getId().equals( searchKey.getId() ) )
                return (int)targetKey.getKey().getStartTime();
        }
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

    public void setFiles( List<FileMetaData> level0s )
    {
        this.files = new ArrayList<FileMetaData>( level0s );
    }

    public void setStart( int start )
    {
        this.start = start;
    }
}
