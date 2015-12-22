package org.act.dynproperty.impl;

import java.io.File;

import org.act.dynproperty.util.FileUtils;
import org.act.dynproperty.util.Slice;
import org.junit.Before;
import org.junit.Test;

public class Level1Test
{
    private final int IDNUM = 1024;
    private final int TIMENUM = 1024*1024;
    private Level0 level;
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
        level = Level0.newInstence( dbDir,0 );
        long starttime = System.currentTimeMillis();
        for( int t = 0; t<TIMENUM; t++ )
        {
            for( long i = 0; i<IDNUM; i++ )
            {
                Slice idSlice = new Slice( 12 );
                idSlice.setLong( 0, i );
                Slice valueSlice = new Slice(12);
                valueSlice.setInt( 0, (int)i*10 );
                level.add( idSlice, ValueType.VALUE, t, valueSlice );
            }
        }
        System.out.println( System.currentTimeMillis() - starttime );
    }
    
    @Test
    public void testPointQuery()
    {
        
    }
}
