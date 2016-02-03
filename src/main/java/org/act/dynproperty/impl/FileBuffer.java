package org.act.dynproperty.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.act.dynproperty.impl.MemTable.MemTableIterator;
import org.act.dynproperty.table.TableComparator;
import org.act.dynproperty.table.UnSortedTable;
import org.act.dynproperty.util.Slice;

public class FileBuffer implements Closeable
{
    private MemTable memTable;
    private UnSortedTable discTable;
    
    public FileBuffer(String filename, String UnSortedTableAbsolutName ) throws IOException
    {
        this.memTable = new MemTable( TableComparator.instence() );
        File tableFile = new File( UnSortedTableAbsolutName );
        if( !tableFile.exists() )
        {
            tableFile.createNewFile(); 
            this.discTable = new UnSortedTable( filename, tableFile );
        }
        else
        {
            this.discTable = new UnSortedTable( filename, tableFile );
            this.discTable.initFromFile( this.memTable );
        }
    }
    
    public void add( Slice key, Slice value ) throws Exception
    {
        discTable.add( key, value );
        this.memTable.add( key, value );
    }
    
    public MemTableIterator iterator()
    {
        return this.memTable.iterator();
    }

    @Override
    public void close() throws IOException
    {
        this.memTable = null;
        this.discTable.close();
    }
}
