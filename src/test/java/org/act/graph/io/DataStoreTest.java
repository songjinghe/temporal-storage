package org.act.graph.io;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.act.graph.io.util.DataRecordGenerator;
import org.act.graph.io.util.Util;
import org.junit.Before;
import org.junit.Test;


public class DataStoreTest
{
    private DataStore dataStore;
    private List<DataRecord> recordlist = new LinkedList<DataRecord>();
    private String dir = "." + Util.SEPERATOR + "target" + Util.SEPERATOR + "test-data";
    
    @Before
    public void setUp()
    {
        for( int i = 0; i<5; i++ )
        {
            recordlist.add( DataRecordGenerator.generatDataRecord() );
        }
    }
    
    @Test
    public void setRecord()
    {
        dataStore = new DataStore( dir );
        DataBlock block = new DataBlock();
        for( DataRecord record : this.recordlist )
            block.append( record );
        try
        {
            dataStore.createBlock( block );
            dataStore.createBlock( block );
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        //ÏÂÒ»¸ö²âÊÔ
        readRecord();
    }

    private void readRecord()
    {
        dataStore = new DataStore( dir );
        try
        {
            DataBlock block = dataStore.getBlock( 1 );
            List<DataRecord> list = block.records();
            for( int i = 0; i<list.size(); i++ )
            {
                Assert.assertTrue( list.get( i ).equals( recordlist.get( i ) ) );
            }
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try
        {
            dataStore.close();
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}














