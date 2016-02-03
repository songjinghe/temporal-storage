package org.act.dynproperty.table;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.act.dynproperty.impl.InternalKey;
import org.act.dynproperty.impl.MemTable;
import org.act.dynproperty.util.Slice;

import static org.act.dynproperty.util.SizeOf.SIZE_OF_INT;

public class UnSortedTable implements Closeable
{
    private FileChannel channel;
    private long writePostion;
    
    public UnSortedTable( String filename, File tableFile ) throws IOException
    {
        this.channel = new RandomAccessFile( tableFile, "rw" ).getChannel();
        this.writePostion = this.channel.size();
    }
    
    public void initFromFile( MemTable table ) throws IOException
    {
        this.channel.position( 0 );
        while( this.channel.position() < this.channel.size() )
        {
            ByteBuffer lengthBuffer = ByteBuffer.allocate( 4 );
            this.channel.read( lengthBuffer );
            lengthBuffer.position( 0 ); 
            ByteBuffer keyBuffer = ByteBuffer.allocate( lengthBuffer.getInt() );
            keyBuffer.position(0);
            this.channel.read( keyBuffer );
            InternalKey key = new InternalKey( keyBuffer.array() );
            lengthBuffer.position( 0 );
            this.channel.read( lengthBuffer );
            lengthBuffer.position(0);
            ByteBuffer valueBuffer = ByteBuffer.allocate( lengthBuffer.getInt() );
            this.channel.read( valueBuffer );
            valueBuffer.position(0);
            Slice value = new Slice(valueBuffer.array());
            table.add( key.encode(), value );
        }
    }
    
    public void add( Slice key, Slice value ) throws Exception
    {
        this.channel.position( writePostion );
        ByteBuffer byteBuffer = ByteBuffer.allocate( key.length() + value.length() + SIZE_OF_INT + SIZE_OF_INT );
        byteBuffer.putInt( key.length() );
        byteBuffer.put( key.getBytes() );
        byteBuffer.putInt( value.length() );
        byteBuffer.put( value.getBytes() );
        byteBuffer.position(0);
        this.channel.write( byteBuffer );
        //this.channel.force( false );
        this.writePostion = this.channel.position();
    }

    @Override
    public void close() throws IOException
    {
        this.channel.close();
    }
}
