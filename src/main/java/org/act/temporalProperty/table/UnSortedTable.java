package org.act.temporalProperty.table;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.Slices;

import static org.act.temporalProperty.util.SizeOf.SIZE_OF_INT;

/**
 * Buffer的备份文件。文件中的写入都是在末尾进行append。文件内数据没有顺序
 */
public class UnSortedTable implements Closeable
{
    private final FileChannelLogWriter log;
    private final File file;

    public UnSortedTable( File tableFile ) throws IOException
    {
        this.file = tableFile;
        this.log = new FileChannelLogWriter(tableFile, 0);
    }

    public void initFromFile( MemTable table ) throws IOException
    {
        FileInputStream inputStream = new FileInputStream(file);
        FileChannel channel = inputStream.getChannel();
        LogReader logReader = new LogReader(channel, null, false, 0);
        Slice record;
        List<Slice> keys = new LinkedList<>();
        List<Slice> values = new LinkedList<>();
        while ((record = logReader.readRecord()) != null) {
            SliceInput in = record.input();
            boolean isCheckPoint = in.readBoolean();
            if(!isCheckPoint) {
                int keyLen = in.readInt();
                Slice key = in.readBytes(keyLen);
                int valLen = in.readInt();
                Slice value = in.readBytes(valLen);
                keys.add(key);
                values.add(value);
            }else{
                Iterator<Slice> keyIter = keys.iterator();
                Iterator<Slice> valIter = values.iterator();
                while(keyIter.hasNext() && valIter.hasNext()) {
                    table.add(keyIter.next(), valIter.next());
                }
                keys = new LinkedList<>();
                values = new LinkedList<>();
            }
        }
        channel.close();
        inputStream.close();
    }

    public void add( Slice key, Slice value ) throws IOException
    {
        DynamicSliceOutput out = new DynamicSliceOutput(key.length()+value.length()+8);
        boolean isCheckPoint = false;
        out.writeBoolean(isCheckPoint);
        out.writeInt(key.length());
        out.writeBytes(key);
        out.writeInt(value.length());
        out.writeBytes(value);
        this.log.addRecord(out.slice(), false);
    }

    public void addCheckPoint() throws IOException {
        Slice checkPoint = new Slice(1);
        checkPoint.setByte(0, 1);
        this.log.addRecord(checkPoint, true);
    }

//    public void initFromFile( MemTable table ) throws IOException
//    {
//        this.channel.position( 0 );
//        while( this.channel.position() < this.channel.size() )
//        {
//            ByteBuffer lengthBuffer = ByteBuffer.allocate( 4 );
//            this.channel.read( lengthBuffer );
//            lengthBuffer.position( 0 );
//            ByteBuffer keyBuffer = ByteBuffer.allocate( lengthBuffer.getInt() );
//            keyBuffer.position(0);
//            this.channel.read( keyBuffer );
//            InternalKey key = new InternalKey( keyBuffer.array() );
//            lengthBuffer.position( 0 );
//            this.channel.read( lengthBuffer );
//            lengthBuffer.position(0);
//            ByteBuffer valueBuffer = ByteBuffer.allocate( lengthBuffer.getInt() );
//            this.channel.read( valueBuffer );
//            valueBuffer.position(0);
//            Slice value = new Slice(valueBuffer.array());
//            table.add( key.encode(), value );
//        }
//    }
//
//    public void add( Slice key, Slice value ) throws IOException
//    {
//        this.channel.position( writePostion );
//        ByteBuffer byteBuffer = ByteBuffer.allocate( key.length() + value.length() + SIZE_OF_INT + SIZE_OF_INT );
//        byteBuffer.putInt( key.length() );
//        byteBuffer.put( key.getBytes() );
//        byteBuffer.putInt( value.length() );
//        byteBuffer.put( value.getBytes() );
//        byteBuffer.position(0);
//        this.channel.write( byteBuffer );
//        //this.channel.force( false );
//        this.writePostion = this.channel.position();
//    }

    @Override
    public void close() throws IOException
    {
        this.log.close();
    }
}
