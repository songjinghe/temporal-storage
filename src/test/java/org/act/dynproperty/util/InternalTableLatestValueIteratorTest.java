package org.act.dynproperty.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;

import org.act.dynproperty.impl.Filename;
import org.act.dynproperty.impl.InternalKey;
import org.act.dynproperty.table.FileChannelTable;
import org.act.dynproperty.table.Table;
import org.act.dynproperty.table.TableComparator;
import org.junit.Test;

public class InternalTableLatestValueIteratorTest
{
    private static final int IDNUM = 30;
    private static final int PRONUM = 512;
    private static final int TIMENUM = 512;
    private static final String dbDir = "./target/testDB";
    
    
    @Test
    public void testPointQuery()
    {
        try
        {
            String fileName = Filename.stableFileName(0);
            File file = new File(dbDir + "/" + fileName );
            FileChannel channel = new FileInputStream( file ).getChannel();
            Table table = new FileChannelTable( fileName, channel, TableComparator.instence(), false );
            //TableIterator iterator = table.iterator();
            InternalTableLatestValueIterator iterator = new InternalTableLatestValueIterator( table.lastestValueIterator() );
            File output = new File( dbDir + "/" + "testoutput" );
            output.delete();
            if( !output.exists() )
                output.createNewFile();
            FileWriter write = new FileWriter( output );
            while( iterator.hasNext() )
            {
                Entry<InternalKey,Slice> entry = iterator.next();
                //InternalKey key = new InternalKey( entry.getKey() );
                write.write( entry.getKey().toString() + "\n" );
            }
            channel.close();
            write.close();
        }
        catch( IOException e)
        {
            e.printStackTrace();
        }
    }
}
