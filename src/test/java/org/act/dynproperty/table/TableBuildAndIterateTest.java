package org.act.dynproperty.table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.act.dynproperty.impl.Filename;
import org.act.dynproperty.impl.Options;
import org.act.dynproperty.util.Slice;
import org.junit.Before;
import org.junit.Test;


public class TableBuildAndIterateTest
{
    private static final String dbDir = "./target/BigFileTest";
    private static final int FILE_SIZE = (int)(1024*1024*10);
    
    
    @Before
    public void buildFile()
    {
        try
        {
            String fileName = Filename.stableFileName( 0 );
            File file = new File( dbDir + fileName );
            file.delete();
            if( !file.exists() )
                file.createNewFile();
            FileChannel channel = new FileOutputStream( file ).getChannel();
            TableBuilder builder = new TableBuilder( new Options(), channel, new BytewiseComparator() );
            for( long i = 0; i<=FILE_SIZE; i++ )
            {
                Slice key = new Slice( 8 );
                key.setLong( 0, i );
                Slice value = new Slice(8);
                value.setLong(0,i);
                builder.add( key, value );
            }
            builder.finish();
            channel.close();
        }
        catch( IOException e ){}
    }
    
    //@Test
    public void singelThreadIterateTable()
    {
        try
        {
            String fileName = Filename.stableFileName( 0 );
            File file = new File( dbDir + fileName );
            FileChannel channel = new FileInputStream( file ).getChannel();
            Table table = new MMapTable( fileName, channel, TableComparator.instence(), false );
            long id = 0;
            for( Entry<Slice,Slice> entry : table )
            {
                Slice idslice = new Slice(8);
                idslice.setLong( 0, id );
                Assert.assertEquals( idslice, entry.getKey() );
                Assert.assertEquals( idslice, entry.getValue() );
                id++;
            }
            channel.close();
        }
        catch( IOException e ){}
    }
    
    @Test
    public void multiThreadIterateTable()
    {
        try
        {
            String fileName = Filename.stableFileName( 0 );
            File file = new File( dbDir + fileName );
            FileChannel channel = new FileInputStream( file ).getChannel();
            Table table = new MMapTable( fileName, channel, TableComparator.instence(), false );
            ExecutorService ex = Executors.newFixedThreadPool( 2 );
            CountDownLatch latch = new CountDownLatch( 2 );
            ex.execute( new Runnable()
            {
                @Override
                public void run()
                {
    
                    long id = 0;
                    for( Entry<Slice,Slice> entry : table )
                    {
                        Slice idslice = new Slice(8);
                        idslice.setLong( 0, id );
                        Assert.assertEquals( idslice, entry.getKey() );
                        Assert.assertEquals( idslice, entry.getValue() );
                        id++;
                    }
                    latch.countDown();
                }
            } );
            ex.execute( new Runnable()
            {
                @Override
                public void run()
                {
    
                    long id = 0;
                    for( Entry<Slice,Slice> entry : table )
                    {
                        Slice idslice = new Slice(8);
                        idslice.setLong( 0, id );
                        Assert.assertEquals( idslice, entry.getKey() );
                        Assert.assertEquals( idslice, entry.getValue() );
                        id++;
                    }
                    latch.countDown();
                }
            } );
            latch.await();
        }
        catch( Exception e )
        {
            e.printStackTrace(  );
        }
    }
}
