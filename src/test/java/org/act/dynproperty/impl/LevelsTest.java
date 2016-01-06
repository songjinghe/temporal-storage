package org.act.dynproperty.impl;

import java.io.File;

import junit.framework.Assert;

import org.act.dynproperty.impl.callback.CountCallBack;
import org.act.dynproperty.util.FileUtils;
import org.act.dynproperty.util.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

public class LevelsTest
{
    private static final int IDNUM = 30;
    private static final int PRONUM = 512;
    private static final int TIMENUM = 512;
    private static Levels level;
    private static final String dbDir = "./target/testDB";
    @BeforeClass
    public static void setUp()
    {
        File dir = new File( dbDir);
        if( !dir.exists() )
        {
            dir.mkdirs();
            //FileUtils.deleteDirectoryContents( dir );
        }
        else {
            FileUtils.deleteDirectoryContents( dir );
        }
        if( null != level )
            return;
        level = new Levels( dbDir );
        long startTime = System.currentTimeMillis();
        for( int t = 0; t<TIMENUM; t++ )
        {
            for( long i = 0; i<IDNUM; i++ )
            {
                for( int p = 0; p<PRONUM; p++ )
                {
                    Slice value = new Slice(16);
                    value.setLong( 0, i );
                    value.setInt( 8, p );
                    value.setInt( 12, t );
                    level.add( i, p, t*10, value );
                }
            }
        }
        System.out.println( (float)IDNUM*(float)PRONUM*(float)TIMENUM/1024.0/1024.0*28.0 + "M data write use time : " + (System.currentTimeMillis() - startTime) + "MillSecond!" );
    }
    
    
    @Test
    public void testRangeQuery()
    {
        long startTime = System.currentTimeMillis();
        for( long i = 0; i<IDNUM; i++ )
        {
            for( int p = 0; p<PRONUM; p++ )
            {
                Slice value = level.getRangeValue( i, p, 5, TIMENUM*10, new CountCallBack() );
                Slice expected = new Slice( 4 );
                expected.setInt( 0, TIMENUM );
                Assert.assertEquals( expected.getInt( 0 ), value.getInt( 0 ) );
            }
        }
        System.out.println( IDNUM*PRONUM + "times random range query use time : " + (System.currentTimeMillis() - startTime) + "MillSecond!" );
    }
    
    
    @Test
    public void testPointQuery()
    {
//        Slice temp = level.getPointValue( 29, 29, 2590 );
//        temp.setByte( 0, 1 );
        long startTime = System.currentTimeMillis();
        for( int t = 0; t<TIMENUM; t++ )
        {
            for( long i = 0; i<IDNUM; i++ )
            {
                for( int p = 0; p<PRONUM; p++ )
                {
                    Slice value = level.getPointValue( i, p, t*10).getValue();
                    long id = value.getLong( 0 );
                    int proid = value.getInt( 8 );
                    int time = value.getInt( 12 );
                    Assert.assertEquals( p, proid );
                    Assert.assertEquals( i, id );
                    Assert.assertEquals( "id=" + i + "pid=" + p +"time=" + t, t, time );
                    //System.out.println( "id=" + i + "pid=" + p +"time=" + t );
                }
            }
        }
        System.out.println( IDNUM*PRONUM*TIMENUM + "times random point query use time : " + (System.currentTimeMillis() - startTime) + "MillSecond!" );
        
    }
}
