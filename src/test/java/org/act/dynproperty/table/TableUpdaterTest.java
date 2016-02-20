package org.act.dynproperty.table;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.act.dynproperty.impl.Filename;
import org.act.dynproperty.impl.InternalKey;
import org.act.dynproperty.impl.Options;
import org.act.dynproperty.impl.SequenceNumber;
import org.act.dynproperty.impl.ValueType;
import org.act.dynproperty.util.Slice;
import org.act.dynproperty.table.TableIterator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableUpdaterTest
{
    
    private static Table table;
    private static final String dbDir = "./target/TableUpdateTest";
    private final static int TIME_NUMS = 100;
    private final static int PRO_NUMS = 50;
    private final static int ID_NUMS = 100;
    
    @BeforeClass
    public static void setUp()
    {
        try
        {
            String fileName = Filename.stableFileName( 0 );
            File file = new File( dbDir + fileName);
            if( file.exists() )
                file.delete();
            file.createNewFile();
            RandomAccessFile randomFile = new RandomAccessFile( file, "rw" );
            FileChannel channel = randomFile.getChannel();
            TableBuilder builder = new TableBuilder( new Options(), channel, TableComparator.instence() );      
            for( long i = 0; i<ID_NUMS; i++ )
            {
                for( int p = 0; p<PRO_NUMS; p++ )
                {
                    for( int t = 0; t<TIME_NUMS; t++ )
                    {
                        Slice idSlice = new Slice( 12 );
                        idSlice.setLong( 0, i );
                        idSlice.setInt( 8, p );
                        Slice value = new Slice( 20 );
                        value.setLong( 0, i );
                        value.setInt( 8, p );
                        long sequence = SequenceNumber.packSequenceAndValueType( t, 20, ValueType.VALUE );
                        value.setLong( 12, sequence );
                        InternalKey key = new InternalKey( idSlice, t, 20, ValueType.VALUE );
                        builder.add( key.encode(), value );
                    }
                }
            }
            builder.finish();
            channel.force( true );
            table = new MMapTable( fileName, channel, TableComparator.instence(), false );
        }
        catch( IOException e ){}
    }
    
    @Test
    public void updateInvalidTest()
    {
        TableUpdater updater = new TableUpdater( table );
          for( long i = 0; i<ID_NUMS; i++ )
          {
              for( int p = 0; p<PRO_NUMS; p++ )
              {
                  for( int t = 0; t<TIME_NUMS; t++ )
                  {
                      Slice idSlice = new Slice(12);
                      idSlice.setLong( 0, i );
                      idSlice.setInt( 8, p );
                      Slice newvalue = new Slice( 20 );
                      newvalue.setLong( 0, i+1 );
                      newvalue.setInt( 8, p+1 );
                      long sequnence = SequenceNumber.packSequenceAndValueType( t, 20, ValueType.DELETION );
                      newvalue.setLong( 12, sequnence );
                      updater.update( idSlice, t, 20, ValueType.INVALID, newvalue );
                  }
              }
          }
          TableIterator iterator = table.iterator();
          for( long i = 0; i<ID_NUMS; i++ )
          {
              for( int p = 0; p<PRO_NUMS; p++ )
              {
                  for( int t = 0; t<TIME_NUMS; t++ )
                  {
                      Entry<Slice,Slice> entry = iterator.next();
                      InternalKey valuekey = new InternalKey( entry.getValue() );
                      InternalKey key = new InternalKey( entry.getKey() );
                      Slice idSlice = new Slice( 12 );
                      idSlice.setLong( 0, i+1 );
                      idSlice.setInt( 8, p+1 );
                      Assert.assertEquals( idSlice, valuekey.getId() );
                      Assert.assertEquals( ValueType.DELETION.getPersistentId(), valuekey.getValueType().getPersistentId() );
                      Assert.assertEquals( ValueType.INVALID.getPersistentId(), key.getValueType().getPersistentId() );
                  }
              }
          }
    }
    
    @Test
    public void updateDeleteTest()
    {
        TableUpdater updater = new TableUpdater( table );
          for( long i = 0; i<ID_NUMS; i++ )
          {
              for( int p = 0; p<PRO_NUMS; p++ )
              {
                  for( int t = 0; t<TIME_NUMS; t++ )
                  {
                      Slice idSlice = new Slice(12);
                      idSlice.setLong( 0, i );
                      idSlice.setInt( 8, p );
                      Slice newvalue = new Slice( 20 );
                      newvalue.setLong( 0, i+1 );
                      newvalue.setInt( 8, p+1 );
                      long sequnence = SequenceNumber.packSequenceAndValueType( t, 20, ValueType.DELETION );
                      newvalue.setLong( 12, sequnence );
                      updater.update( idSlice, t, 20, ValueType.DELETION, newvalue );
                  }
              }
          }
          TableIterator iterator = table.iterator();
          for( long i = 0; i<ID_NUMS; i++ )
          {
              for( int p = 0; p<PRO_NUMS; p++ )
              {
                  for( int t = 0; t<TIME_NUMS; t++ )
                  {
                      Entry<Slice,Slice> entry = iterator.next();
                      InternalKey valuekey = new InternalKey( entry.getValue() );
                      InternalKey key = new InternalKey( entry.getKey() );
                      Slice idSlice = new Slice( 12 );
                      idSlice.setLong( 0, i+1 );
                      idSlice.setInt( 8, p+1 );
                      Assert.assertEquals( idSlice, valuekey.getId() );
                      Assert.assertEquals( ValueType.DELETION.getPersistentId(), valuekey.getValueType().getPersistentId() );
                      Assert.assertEquals( ValueType.DELETION.getPersistentId(), key.getValueType().getPersistentId() );
                  }
              }
          }
    }
    
    @Test
    public void updateValueAndTest()
    {
        TableUpdater updater = new TableUpdater( table );
        for( long i = 0; i<ID_NUMS; i++ )
        {
            for( int p = 0; p<PRO_NUMS; p++ )
            {
                for( int t = 0; t<TIME_NUMS; t++ )
                {
                    Slice idSlice = new Slice(12);
                    idSlice.setLong( 0, i );
                    idSlice.setInt( 8, p );
                    Slice newvalue = new Slice( 20 );
                    newvalue.setLong( 0, i+1 );
                    newvalue.setInt( 8, p+1 );
                    long sequnence = SequenceNumber.packSequenceAndValueType( t, 20, ValueType.DELETION );
                    newvalue.setLong( 12, sequnence );
                    updater.update( idSlice, t, 20, ValueType.VALUE, newvalue );
                }
            }
        }
        TableIterator iterator = table.iterator();
        for( long i = 0; i<ID_NUMS; i++ )
        {
            for( int p = 0; p<PRO_NUMS; p++ )
            {
                for( int t = 0; t<TIME_NUMS; t++ )
                {
                    Entry<Slice,Slice> entry = iterator.next();
                    InternalKey valuekey = new InternalKey( entry.getValue() );
                    Slice idSlice = new Slice( 12 );
                    idSlice.setLong( 0, i+1 );
                    idSlice.setInt( 8, p+1 );
                    Assert.assertEquals( idSlice, valuekey.getId() );
                    Assert.assertEquals( ValueType.DELETION.getPersistentId(), valuekey.getValueType().getPersistentId() );
                }
            }
        }
    }
}
