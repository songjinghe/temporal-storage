package org.act.temporalProperty.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.act.temporalProperty.impl.FileBuffer;
import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.impl.Options;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.table.BufferFileAndTableIterator;
import org.act.temporalProperty.table.MMapTable;
import org.act.temporalProperty.table.Table;
import org.act.temporalProperty.table.TableBuilder;
import org.act.temporalProperty.table.TableComparator;
import org.junit.BeforeClass;
import org.junit.Test;

public class BufferFileAndTableIteratorTest
{
    private static final String dbDir = "./target/BufferFileAndTableIteratorTest";
    private static final String bufferfileName = "buffer.test";
    private static final String tablefileName = "table.test";
    private static final int DATA_SIZE = 100000;
    private static FileBuffer buffer;
    private static Table table;
    private static MemTable memTable;
    
    @BeforeClass
    public static void setUp()
    {
        TableBuilder builder;
        try
        {
            File bufferFile = new File( dbDir + bufferfileName );
            File tableFile = new File( dbDir + tablefileName );
            if( bufferFile.exists() )
                bufferFile.delete();
            bufferFile.createNewFile();
            if( tableFile.exists() )
                tableFile.delete();
            tableFile.createNewFile();
            FileChannel bufferChannel = new RandomAccessFile( bufferFile, "rw" ).getChannel();
            FileChannel tableChannel = new RandomAccessFile( tableFile,"rw").getChannel();
            buffer = new FileBuffer( bufferfileName, dbDir + tablefileName );
            builder = new TableBuilder( new Options(), tableChannel, TableComparator.instance() );
            memTable = new MemTable( TableComparator.instance() );
            for( int i = 0; i<DATA_SIZE; i++ )
            {
                Slice key = new Slice(20);
                key.setInt( 0, i );
                Slice value = new Slice(4);
                value.setInt(0,i);
                if( i % 3 == 0 )
                {
                    buffer.add( key, value );
                }
                else if( i % 3 == 1)
                {
                    builder.add( key, value );
                }
                else
                    memTable.add( key, value );
            }
            builder.finish();
            table = new MMapTable( tablefileName, tableChannel, TableComparator.instance(), false );
        }
        catch( Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void test()
    {
        List<SeekingIterator<Slice,Slice>> list = new LinkedList<SeekingIterator<Slice,Slice>>(); 
        BufferFileAndTableIterator iterator = new BufferFileAndTableIterator( buffer.iterator(), table.iterator(), TableComparator.instance() );
        list.add( iterator );
        list.add( memTable.iterator() );
        MergingIterator merge = new MergingIterator( list, TableComparator.instance() );
        int expected = 0;
        while( merge.hasNext() )
        {
            Entry<Slice,Slice> entry = merge.next();
            Assert.assertEquals( expected, entry.getKey().getInt( 0 ) );
            Assert.assertEquals( expected, entry.getValue().getInt( 0 ) );
            expected++;
        }
    }
}





















