package org.act.temporalProperty.table;

import java.io.File;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.impl.MemTable.MemTableIterator;
import org.act.temporalProperty.util.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

public class UnSortTableTest
{
    private static final String dbDir = "./target/TableUpdateTest";
    private static String fileName = "temp.test";
    private static final int DATA_SIZE = 200000;
    private static UnSortedTable table;
    
    @BeforeClass
    public static void setUp()
    {
        try
        {
            File file = new File( dbDir + fileName );
            if( file.exists() )
                file.delete();
            file.createNewFile();
            table = new UnSortedTable( fileName, file );
            for( int i = 0; i<DATA_SIZE; i++ )
            {
                Slice key = new Slice( 20 );
                key.setLong( 0, i );
                Slice value = new Slice(4);
                value.setInt( 0, i );
                table.add( key, value );
            }
        }
        catch( Throwable t ){}
    }
    
    @Test
    public void test()
    {
        MemTable memtable = new MemTable( TableComparator.instence() );
        try
        {
            table.initFromFile( memtable );
            MemTableIterator iterator = memtable.iterator();
            for( int i = 0; i<DATA_SIZE; i++ )
            {
                Entry<Slice,Slice> entry = iterator.next();
                Assert.assertEquals( entry.getKey().getLong( 0 ), (long)i );
                Assert.assertEquals( entry.getValue().getInt( 0 ), i );
            }
        }
        catch ( Exception e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}











