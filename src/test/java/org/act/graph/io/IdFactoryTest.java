package org.act.graph.io;

import java.io.IOException;

import junit.framework.Assert;

import org.act.graph.io.util.Util;
import org.junit.Before;
import org.junit.Test;

/**
 *
 *
 * @author huanghx( huanghx@act.buaa.edu.cn )
 */
public class IdFactoryTest
{
    private IdFactory factory;
    
    @Before
    public void setup()
    {
        try
        {
            factory = new IdFactory( "." + Util.SEPERATOR + "target" + Util.SEPERATOR + "test-data", "test" );
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @Test
    public void testFactoryInit()
    {
        try
        {
            long id1 = this.factory.allocateId();
            Assert.assertEquals( 0, id1 );
            long id2 = this.factory.allocateId();
            Assert.assertEquals( 1, id2 );
            this.factory.allocateId();
            this.factory.returnId( id1 );
            this.factory.returnId( id2 );
            Assert.assertEquals( this.factory.allocateId(), id2 );
            this.factory.close();
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
