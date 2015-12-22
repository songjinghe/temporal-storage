package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.act.dynproperty.util.TableIterator;

import com.google.common.base.Preconditions;

public class Level0
{
    private List<FileMetaData> files = new ArrayList<FileMetaData>();
    private MemTable memTable;
    private MemTable stableTable;
    private static Lock lock = new ReentrantLock();
    private String dbDir;
    
    private int lastAddTime = 0;
    private int start;
    
    private static Level0 instence = null;
    private Level0( String dbDir, int startTime )
    {
        this.dbDir = dbDir;
        memTable = new MemTable( MemTableComparator.instence(), startTime );
        for(int i =0; i<5; i++ )
            files.add( null );
    }
    
    public static Level0 newInstence( String dbDir, int startTime )
    {
        if( instence == null )
        {
            lock.lock();
            if( instence != null )
            {
                lock.unlock();
                return instence;
            }
            else
            {
                instence = new Level0( dbDir, startTime );
                lock.unlock();
                return instence;
            }
        }
        else
            return instence;
    }
    
    public Slice getPointValue( Slice id, int time )
    {
        lock.lock();
        if( time >= this.memTable.getStartTime() )
        {
            LookupKey lookupKey = new LookupKey( id, time );
            lock.unlock();
            return this.memTable.get( lookupKey ).getValue();
        }
        else
        {
            FileMetaData target = null;
            for( FileMetaData meta : this.files )
            {
                if( null != meta )
                {
                    if( time >= meta.getSmallest() && time < meta.getLargest() )
                    {
                        target = meta;
                        break;
                    }
                }
            }
            String fileName = Filename.unStableFileName( target.getNumber() );
            try( FileChannel channel = new FileInputStream( new File( this.dbDir + "/" + fileName ) ).getChannel() )
            {
                //FileChannel channel = new FileInputStream( new File( this.dbDir + "/" + fileName ) ).getChannel();
                Table table = new FileChannelTable( fileName, channel, TableComparator.instence(), false );
                TableIterator iterator = table.iterator();
                iterator.seek( new InternalKey( id, time, ValueType.VALUE ).encode() );
                Entry<Slice,Slice> entry = iterator.next();
                Slice key = entry.getKey();
                lock.unlock();
                Slice rid = key.copySlice( 0, 12 );
                Preconditions.checkArgument( id.equals( rid ), "Get value faild because returned id is wrong in point query of level0");
                return entry.getValue();
            }
            catch ( IOException e )
            {
                //FIXME
                e.printStackTrace();
            }
            
        }
        lock.unlock();
        return null;
    }
    
    public int getStart()
    {
        return this.start;
    }
    
    public void add( Slice id,  ValueType valueType, int startTime, Slice value )
    {
        lock.lock();
        if( this.memTable.approximateMemoryUsage() >= 4*1024*1024 && lastAddTime < startTime )
        {
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
        }
        else
        {
            this.memTable.add( id, valueType, startTime, value );
        }
        lastAddTime = startTime;
        lock.unlock();
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
                versionEdit.setNextFileNumber( Level1.instence( dbDir ).getNextFileNum() );
                versionEdit.setPreviousLogNumber( 0 );
                versionEdit.addFile( 0, newMetaData );
                Logs.createLogWriter( dbDir ).addRecord( versionEdit.encode(), true );
                
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
            int startTime = this.files.get( level-1 ).getSmallest();
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
                versionEdit.setNextFileNumber( Level1.instence( dbDir ).getNextFileNum() );
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
                Logs.createLogWriter( dbDir ).addRecord( versionEdit.encode(), true );
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
        int startTime = this.files.get( 4 ).getSmallest();
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
            long fileNum = Level1.instence(this.dbDir).getNextFileNum();
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
            versionEdit.setNextFileNumber( Level1.instence( dbDir ).getNextFileNum() );
            versionEdit.setPreviousLogNumber( 0 );
            for( int i = 0; i<5; i++ )
            {
                File delete = new File(this.dbDir + "/" + Filename.unStableFileName( i ));
                stream2delete.get( i ).close();
                delete.delete();
                versionEdit.deleteFile( 0, this.files.get( i ).getNumber() );
                this.files.set( i, null );
            }
            Level1.instence( this.dbDir ).addFile( metaData );
            versionEdit.addFile( 1, metaData );
            Logs.createLogWriter( dbDir ).addRecord( versionEdit.encode(), true );
        }
        catch( IOException e )
        {
            //FIXME
            e.printStackTrace();
        }
    }

    public Slice getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        Slice idSlice = new Slice(12);
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        for( int i = 4; i >= 0; i-- )
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
        if( endTime >= this.memTable.getStartTime() )
        {
            MemTableIterator iterator = this.memTable.iterator();
            InternalKey startkey = new InternalKey( idSlice, this.memTable.getStartTime(), ValueType.VALUE );
            iterator.seek( startkey );
            while( iterator.hasNext() )
            {
                Entry<InternalKey,Slice> entry = iterator.next();
                Preconditions.checkArgument( entry.getKey().getId().equals( idSlice ), "Get value faild because returned id is wrong in range query of level0 int memtable" );
                if( entry.getKey().getStartTime() <= endTime )
                    callback.onCall( entry.getValue() );
                else
                    break;
            }
        }
        return callback.onReturn();
    }
}
