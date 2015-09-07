package org.act.graph.io;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 *
 *代表一个DataBlock，在初始化的时候会放弃Block中空的record
 *对Block的任何修改都通过这个类，然后再用data方法返回需要写入的buffer，
 *这个buffer包括了空的record，长度是整个block的大小
 *
 * @author huanghx( huanghx@act.buaa.edu.cn )
 */
public class DataBlock
{
    
    private final static int BLOCK_RECORD_NUM = 50;
    private final static int BLOCK_HEAD_LEN = 4;
    public final static int BLOCK_SIZE = BLOCK_HEAD_LEN + DataRecord.DATA_RECORD_LEN * BLOCK_RECORD_NUM;
    
    private int recordNum;
    private List<DataRecord> records = new LinkedList<DataRecord>();
    
    private boolean inited = false;
    
    public DataBlock( ByteBuffer buffer )
    {
        buffer.flip();
        this.recordNum = buffer.getInt();
        for( int i = 0; i<recordNum; i++ )
        {
            long start = buffer.getLong();
            long end = buffer.getLong();
            byte[] data = new byte[DataRecord.DATA_LENGTH];
            buffer.get( data );
            DataRecord record = new DataRecord( start, end, data );
            records.add( record );
        }
        inited = true;
    }
    
    public DataBlock()
    {
        this.recordNum = 0;
        inited = true;
    }
    
    /**
     * 
     * 是否会造成满需要在调用这个方法之前判断，本方法不做相应操作
     * 
     * @param record 放在末尾的record
     * @return 如果成功返回true，如果失败返回false，失败的原因是还没有初始化完全
     */
    public boolean append( DataRecord record )
    {
        if( !inited )
            return false;
        this.records.add( record );
        this.recordNum++;
        return true;
    }
    
    
    /**
     * @return 包涵全部block的ByteBuffer，在写入文件前先flip()
     */
    public ByteBuffer data()
    {
        ByteBuffer toret = ByteBuffer.allocate( BLOCK_SIZE );
        toret.putInt( this.recordNum );
        assert this.records.size() == this.recordNum;
        for( DataRecord record : this.records )
        {
            toret.put( record.getbytes() );
        }
//        for( int i = 0; i<BLOCK_RECORD_NUM-this.recordNum; i++ )
//        {
//            toret.put( new byte[DataRecord.DATA_RECORD_LEN] );
//        }
        return toret;
    }
    
    public List<DataRecord> records()
    {
        return this.records;
    }
    
}






