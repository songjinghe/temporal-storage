package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.act.dynproperty.DynPropertyStore;
import org.act.dynproperty.table.MergeProcess;
import org.act.dynproperty.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynPropertyStoreImpl implements DynPropertyStore
{
    private UnstableLevel unLevel;
    private StableLevel stlevel;
    private MergeProcess mergeProcess;
    private String dbDir;
    private Logger log = LoggerFactory.getLogger( DynPropertyStoreImpl.class );
    
    public DynPropertyStoreImpl( String dbDir )
    {
        this.dbDir = dbDir;
        ReadWriteLock fileMetaLock = new ReentrantReadWriteLock( true );
        this.stlevel = new StableLevel( dbDir,fileMetaLock );
        this.mergeProcess = new MergeProcess( dbDir, this.stlevel,fileMetaLock );
        this.unLevel = new UnstableLevel( dbDir, this.mergeProcess,fileMetaLock, this.stlevel );
        Runtime.getRuntime().addShutdownHook( new Thread(){
            public void run()
            {
                DynPropertyStoreImpl.this.stop();
            }
        });
        start();
    }
    
    private void stop()
    {
        this.unLevel.dumpMemTable2disc();
        this.unLevel.dumpFileMeta2disc();
        this.stlevel.dumFileMeta2disc();
    }
    
    private void start()
    {
        loadExitingFilesFromdisc();
        this.unLevel.restoreMemTable();
    }


    private void loadExitingFilesFromdisc()
    {
        try
        {
            // unstable 
            String logFileName = Filename.logFileName( 0 );
            File logFile = new File( this.dbDir + "/" + logFileName );
            if( !logFile.exists() )
            {
                return;
            }
            FileInputStream inputStream = new FileInputStream( logFile );
            FileChannel channel = inputStream.getChannel();
            LogReader logReader = new LogReader( channel, null, false, 0 );
            for( Slice logRecord = logReader.readRecord(); logRecord != null; logRecord = logReader.readRecord() )
            {
                VersionEdit edit = new VersionEdit( logRecord );
                for( Entry<Integer,FileMetaData> entry : edit.getNewFiles().entries() )
                {
                    this.unLevel.initfromdisc( entry.getValue() );
                }
            }
            inputStream.close();
            channel.close();
            Files.delete( logFile.toPath() );
            
            // stable
            logFileName = Filename.logFileName( 1 );
            logFile = new File( this.dbDir + "/" + logFileName );
            if( !logFile.exists() )
            {
                return;
            }
            inputStream = new FileInputStream( logFile );
            channel = inputStream.getChannel();
            logReader = new LogReader( channel, null, false, 0 );
            for( Slice logRecord = logReader.readRecord(); logRecord != null; logRecord = logReader.readRecord() )
            {
                VersionEdit edit = new VersionEdit( logRecord );
                for( Entry<Integer,FileMetaData> entry : edit.getNewFiles().entries() )
                {
                    this.stlevel.initfromdisc( entry.getValue() );
                }
            }
            inputStream.close();
            channel.close();
            Files.delete( logFile.toPath() );
            
        }
        catch( IOException e )
        {
            //FIXME
            log.error( "PropertyStore Init fails when retore existing file's info!" );
        }
    }


    @Override
    public Slice getPointValue( long id, int proId, int time )
    {
        Slice idSlice = new Slice( 12 );
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        if( time >= this.stlevel.getTimeBoundary() )
        {
            Slice toret = this.unLevel.getPointValue( idSlice, time );
            if( null == toret )
                return this.stlevel.getPointValue( idSlice, time );
            else if( toret.length() == 0 )
                return null;
            else
                return toret;
        }
        else
            return this.stlevel.getPointValue( idSlice, time );
    }
    @Override
    public Slice getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        Slice idSlice = new Slice( 12 );
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        if( startTime < this.stlevel.getTimeBoundary() )
            this.stlevel.getRangeValue( idSlice, startTime, Math.min( this.stlevel.getTimeBoundary(), endTime ), callback );
        if( endTime >= this.stlevel.getTimeBoundary() )
            this.unLevel.getRangeValue( idSlice, Math.max( this.stlevel.getTimeBoundary(), startTime ), endTime, callback );
        return callback.onReturn();
    }

    @Override
    public boolean setProperty( Slice key, byte[] value )
    {
        Slice valueSlice = new Slice( value );
        InternalKey internalKey = new InternalKey( key );
        return this.unLevel.set( internalKey, valueSlice );
    }

    @Override
    public boolean delete( Slice id )
    {
        // TODO Auto-generated method stub
        return false;
    }
    
}
