package org.act.dynproperty.impl;

import java.io.File;

import junit.framework.Assert;

import org.act.dynproperty.util.FileUtils;
import org.act.dynproperty.util.Slice;
import org.junit.Before;
import org.junit.Test;

public class LevelsTest
{
    private final int IDNUM = 512;
    private final int PRONUM = 256;
    private final int TIMENUM = 32;
    private Levels level;
    private final String dbDir = "./testDB";
    @Before
    public void setUp()
    {
        File dir = new File( dbDir);
        if( dir.exists() )
        {
            FileUtils.deleteDirectoryContents( dir );
        }
        dir.mkdirs();
        level = Levels.instence( dbDir );
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
    }
    
    @Test
    public void testPointQuery()
    {
        for( int t = 0; t<TIMENUM; t++ )
        {
            for( long i = 0; i<IDNUM; i++ )
            {
                for( int p = 0; p<PRONUM; p++ )
                {
                    Slice value = level.getPointValue( i, p, t*10+5 );
                    long id = value.getLong( 0 );
                    int proid = value.getInt( 8 );
                    int time = value.getInt( 12 );
                    Assert.assertEquals( p, proid );
                    Assert.assertEquals( i, id );
                    Assert.assertEquals( "id=" + i + "pid=" + p +"time=" + t, t, time );
                    System.out.println( "id=" + i + "pid=" + p +"time=" + t );
                }
            }
        }
        Slice value = level.getPointValue( 511, 255, 155 );
        long id = value.getLong( 0 );
        int proid = value.getInt( 8 );
        int time = value.getInt( 12 );
    }
}
