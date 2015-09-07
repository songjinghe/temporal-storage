package org.act.graph.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.act.graph.io.util.Util;

/**
 *
 *DataStore目前就是DataFile，目前DataFile没有头信息，直接从0开始第一个Block
 *目前只提供单个Block的读和写和新建，读写都需要给定offset。新建需要从Idfactory得到分配的offset
 *
 * @author huanghx( huanghx@act.buaa.edu.cn )
 */
public class DataStore implements Closeable
{
    
    private String dir;
    
    private IdFactory factory;
    
    private File datafile;
    private FileChannel channel;
    
    public DataStore( String d )
    {
        this.dir = d;
        try
        {
            factory = new IdFactory( dir, "data" );
            datafile = new File( dir + Util.SEPERATOR + "data.file" );
            if( !datafile.exists() )
                datafile.createNewFile();
            channel = new RandomAccessFile( datafile, "rw" ).getChannel();
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    
    public DataBlock getBlock( long offset ) throws IOException
    {
        long position = offset * DataBlock.BLOCK_SIZE;
        this.channel.position( position );
        ByteBuffer buffer = ByteBuffer.allocate( DataBlock.BLOCK_SIZE );
        int sum = 0;
        while( ( sum += this.channel.read( buffer ) ) <DataBlock.BLOCK_SIZE && this.channel.position() < this.channel.size() ) {};
        DataBlock toret = new DataBlock( buffer );
        return toret;
    }
    
    public void setBlock( DataBlock block, long offset ) throws IOException
    {
        this.channel.position( offset );
        ByteBuffer buffer = block.data();
        buffer.flip();
        while( buffer.hasRemaining() ) this.channel.write( buffer );
    }
    
    public long createBlock( DataBlock block ) throws IOException
    {
        long offset = this.factory.allocateId();
        this.channel.position( offset*DataBlock.BLOCK_SIZE );
        ByteBuffer buffer = block.data();
        buffer.flip();
        while( buffer.hasRemaining() ) this.channel.write( buffer );
        return offset;
    }
    
    public void deleteBlock( long offset )
    {
        this.factory.returnId( offset );
    }
    
    /**
     * 
     * 只有在系统关闭时不再需要Store的时候再调用。
     * 如果在close之后再调用其他的方法都会出现异常。
     * 
     * @throws IOException
     */
    public void close() throws IOException
    {
        this.factory.close();
        this.channel.force( true );
        this.channel.close();
        this.datafile = null;
        this.channel = null;
    }
    
    
    
}






