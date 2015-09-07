package org.act.graph.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.EmptyStackException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.act.graph.io.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这里Id的值，是指对应数据文件中数据block的offset
 * 
 *Id分配文件，在文件中的构造如下：
 * **********************
 * *-8bytes-higtest id--*
 * *--------------------*
 * *-4bytes-empty id num*
 * *--------------------*
 * *-8bytes-empty id----*
 * *         .          *
 * *         .          *
 * *--------------------*
 *
 * 最高的Id从0开始分配，分配一次加1
 *
 * @author huanghx( huanghx@act.buaa.edu.cn )
 * 
 * 
 */
//TODO 仅仅在close的时候把信息存储在文件中，如果过程中出问题就会丢失信息！！
public class IdFactory implements Closeable
{
    
    private Logger logger = LoggerFactory.getLogger( IdFactory.class );
    
    private File file;
    private String factoryname;
    private FileChannel filechannel;
    
    private static final int HEADER_LEN = 12;
    private AtomicLong highestid = new AtomicLong();
    private Stack<Long> reusedIdstack = new Stack<Long>();
    
    private boolean available = false;
    
    public IdFactory( String dictionary , String factoryname ) throws IOException
    {
        this.factoryname = factoryname + ".id";
        this.file = new File( dictionary + Util.SEPERATOR + this.factoryname );
        init();
        initreadfromfile();
        this.available = true;
    }

    
    public void returnId( long id )
    {
        //TODO 不合理
        if(!available)
            return;
        this.reusedIdstack.push( id );
    }
    
    /**
     * @return -1如果初始化还没有完成，或者正在销毁。
     */
    public long allocateId()
    {
        if( !available )
            return -1;
        long toret;
        try
        {
            toret = this.reusedIdstack.pop();
        }
        catch( EmptyStackException e )
        {
            toret = this.highestid.getAndIncrement();
        }
        return toret;
    }
    
    
    /**
     * 
     * 只有在系统关闭时不再需要IdFactory的时候再调用。
     * 如果在close之后再调用其他的方法都会出现异常。
     * 
     * @throws IOException
     */
    public void close() throws IOException
    {
        this.available = false;
        try
        {
            flushToFile();
            this.filechannel.force( true );
            this.filechannel.close();
        }
        catch ( IOException e )
        {
            logger.error( this.factoryname + "Id文件close出错！" );
            throw e;
        }
        this.reusedIdstack = null;
        this.highestid = null;
        this.file = null;
        this.filechannel = null;
    }
    
    private void flushToFile() throws IOException
    {
        //写最高id
        this.filechannel.position( 0 );
        ByteBuffer buffer = ByteBuffer.allocate( 8 );
        buffer.putLong( this.highestid.get() );
        buffer.flip();
        while( buffer.hasRemaining() ) this.filechannel.write( buffer );
        
        //写重用id个数
        int num = this.reusedIdstack.size();
        buffer = ByteBuffer.allocate( 4 );
        buffer.putInt( num );
        buffer.flip();
        while( buffer.hasRemaining() ) this.filechannel.write( buffer );
        
        //写重用的id
        buffer = ByteBuffer.allocate( 8 );
        List<Long> list = new LinkedList<Long>(this.reusedIdstack);
        for( long l : list )
        {
            buffer.position(0);
            buffer.putLong( l );
            buffer.flip();
            while( buffer.hasRemaining() ) this.filechannel.write( buffer );
        }
        
    }


    private void initreadfromfile() throws IOException
    {
        try
        {
            ByteBuffer buffer = ByteBuffer.allocate( 8 );
            //读最高的值
            this.filechannel.position( 0 );
            int read =0;
            while( ( read += this.filechannel.read( buffer ) ) < 8 ){}
            buffer.flip();
            this.highestid.set( buffer.getLong() );
            
            //读复用的个数
            buffer = ByteBuffer.allocate( 4 );
            read =0;
            while( ( read += this.filechannel.read( buffer ) ) < 4 ){}
            buffer.flip();
            int reuesdIdNum = buffer.getInt();
            
            //读复用的值
            buffer = ByteBuffer.allocate( 8 );
            while( (reuesdIdNum--) > 0 )
            {
                buffer.flip();
                read = 0;
                while( ( read += this.filechannel.read( buffer ) ) < 8 ){}
                buffer.flip();
                long reusedId = buffer.getLong();
                this.reusedIdstack.push( reusedId );
            }
        }
        catch ( IOException e )
        {
            logger.error( this.factoryname + "Id文件初始读失败！" );
            throw e;
        }
        
    }

    
    private void newfilewrite() throws IOException
    {
        //写HighId
        ByteBuffer buffer = ByteBuffer.allocate( 8 );
        buffer.putLong( 0 );
        buffer.flip();
        this.filechannel.position( 0 );
        while( buffer.hasRemaining() ) this.filechannel.write( buffer );
        //写ReusedId个数
        buffer = ByteBuffer.allocate( 4 );
        buffer.putInt( 0 );
        buffer.flip();
        while( buffer.hasRemaining() ) this.filechannel.write( buffer );
    }
    
    
    private void init() throws IOException
    {
        if( !this.file.exists() )
        {
            try
            {
                this.file.createNewFile();
                this.filechannel = new RandomAccessFile( this.file, "rw" ).getChannel();
                newfilewrite();
            }
            catch ( IOException e )
            {
                logger.error( this.factoryname + "Id文件创建失败" );
                throw e;
            }
        }
        this.filechannel = new RandomAccessFile( this.file, "rw" ).getChannel();
    }
    
}
