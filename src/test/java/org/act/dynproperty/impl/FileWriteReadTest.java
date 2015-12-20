package org.act.dynproperty.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.act.dynproperty.table.BytewiseComparator;
import org.act.dynproperty.table.FileChannelTable;
import org.act.dynproperty.util.FileUtils;
import org.act.dynproperty.util.Slice;
import org.act.dynproperty.util.TableIterator;
import org.junit.Before;
import org.junit.Test;

public class FileWriteReadTest
{
    private List<Long> ids = new LinkedList<Long>();
    private List<Integer> times = new LinkedList<Integer>();
    private final int NUM = 10;
    private Level0 level;
    private final String dbDir = "./testDB";
    private Method newMemTableMethod;
    
    @Before
    public void setUp()
    {
        File dir = new File( dbDir);
        if( dir.exists() )
        {
            FileUtils.deleteDirectoryContents( dir );
        }
        dir.mkdirs();
        level = Level0.newInstence( dbDir,0 );
        for( long i = 0; i < NUM; i++ )
        {
            ids.add( i );
            times.add( (int)i * 10 );
        }
        for( int i = 0; i<ids.size(); i++ )
        {
            for( int t = 0; t<times.size(); t++ )
            {
                long id = ids.get( i );
                int time = times.get( t );
                Slice idSlice = new Slice( 12 );
                idSlice.setLong( 0, id );;
                Slice valueSlice = new Slice( 4 );
                valueSlice.setInt( 0, time );
                level.add( idSlice, ValueType.VALUE, time, valueSlice );
            }
        }
        try
        {
            newMemTableMethod = Level0.class.getDeclaredMethod( "newMemTable", int.class );
            newMemTableMethod.setAccessible( true );
        }
        catch ( NoSuchMethodException | SecurityException e )
        {
            e.printStackTrace();
        }
        try
        {
            newMemTableMethod.invoke( level, 100 );
        }
        catch ( IllegalAccessException | IllegalArgumentException | InvocationTargetException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @Test
    public void test()
    {
        File file = new File(this.dbDir + "/" + Filename.unStableFileName( 0 ) );
        try
        {
            FileInputStream stream = new FileInputStream( file );
            FileChannel channel = stream.getChannel();
            FileChannelTable table = new FileChannelTable( file.getAbsolutePath(), channel, new BytewiseComparator(), false );
            TableIterator interator = table.iterator();
            for( int i = 0; i< NUM; i++ )
            {
                for( int t = 0; t<times.size(); t++ )
                {
                    long id = ids.get( i );
                    int time = times.get( t );
                    Slice idSlice = new Slice( 12 );
                    idSlice.setLong( 0, id );
                    InternalKey internalKey = new InternalKey( idSlice, time, ValueType.VALUE );
                    Slice keySlice = internalKey.encode();
                    interator.seek( keySlice );
                    Entry<Slice,Slice> entry = interator.peek();
                    InternalKey answerKey = new InternalKey( entry.getKey() );
                    Assert.assertEquals( idSlice, answerKey.getId() );
                    Assert.assertEquals( (int)times.get( t ), entry.getValue().getInt( 0 ) );
                }
            }
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
