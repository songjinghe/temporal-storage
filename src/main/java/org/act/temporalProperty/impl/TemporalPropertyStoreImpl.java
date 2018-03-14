package org.act.temporalProperty.impl;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.helper.StoreInitial;
import org.act.temporalProperty.meta.SystemMeta;
import org.act.temporalProperty.meta.SystemMetaController;
import org.act.temporalProperty.table.MergeProcess;
import org.act.temporalProperty.table.TableBuilder;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * DynPropertyStore的实现类
 *
 */
public class TemporalPropertyStoreImpl implements TemporalPropertyStore
{
    private SystemMeta meta;
    private MergeProcess mergeProcess;
    private File dbDir;
    private Logger log = LoggerFactory.getLogger( TemporalPropertyStoreImpl.class );
    private Map<Integer, SingleTPStore> properties = new HashMap<>();
    private MemTable memTable;
    /**
     * @param dbDir 存储动态属性数据的目录地址
     */
    public TemporalPropertyStoreImpl( File dbDir ) throws Throwable
    {
        this.dbDir = dbDir;
        start();
    }

    /**
     * 系统启动时调用，主要作用是将上次系统关闭时写入磁盘的数据读入内存
     */
    private void start() throws Throwable
    {
        StoreInitial starter = new StoreInitial(dbDir);
        this.meta = starter.getMetaInfo();
        this.memTable = starter.getMemTable();
    }

    /**
     * 退出系统时调用，主要作用是将内存中的数据写入磁盘。
     */
    public void shutDown()
    {
        this.flushMemTable2Disk();
        this.flushMetaInfo2Disk();
    }


    @Override
    public Slice getPointValue( long id, int proId, int time ) {
        InternalKey searchKey = new InternalKey( toSlice(proId, id), time, 0, ValueType.VALUE );
        this.meta.lockShared();
        List<Map.Entry<Slice,Slice>> candidates = new LinkedList<Map.Entry<Slice,Slice>>();
        {
            try
            {
                SeekingIterator<Slice,Slice> iterator = this.memTable.iterator();
                iterator.seek( searchKey.encode() );
                Map.Entry<Slice,Slice> entry = iterator.next();
                InternalKey answerKey = new InternalKey( entry.getKey() );
                if( answerKey.getId().equals( idSlice ) && answerKey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId()  )
                {
                    candidates.add( entry );
                }
            }
            catch( NoSuchElementException e){}
            try
            {
                if( this.stableMemTable != null )
                {
                    SeekingIterator<Slice,Slice> iterator = this.stableMemTable.iterator();
                    iterator.seek( searchKey.encode() );
                    Map.Entry<Slice,Slice> entry = iterator.next();
                    InternalKey answerKey = new InternalKey( entry.getKey() );
                    if( answerKey.getId().equals( idSlice ) && answerKey.getValueType().getPersistentId() != ValueType.DELETION.getPersistentId()  )
                    {
                        candidates.add( entry );
                    }
                }
            }
            catch( NoSuchElementException e){}
        }
        Slice idSlice =
        Slice result = memTable.get(idSlice);
        if(result!=null) {
            return result;
        }else{
            return properties.get(proId).getPointValue(id, time);
        }
    }

    @Override
    public Object getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback ) {
        Slice idSlice = toSlice(proId, id);
        Slice result = memTable.get(idSlice);
        if(result!=null) {
            return result;
        }else {
            return properties.get(proId).getRangeValue(id, startTime, endTime, callback);
        }
    }

    @Override
    public boolean setProperty( Slice key, byte[] value )
    {
        Slice valueSlice = new Slice( value );
        InternalKey internalKey = new InternalKey( key );
        return this.memTable.set( internalKey, valueSlice );
    }

    @Override
    public boolean delete( Slice id )
    {
        // TODO Auto-generated method stub
        return false;
    }


    public static Slice toSlice(int proId, long id) {
        Slice result = new Slice(12);
        result.setInt( 0, proId );
        result.setLong( 4, id );
        return result;
    }

    /**
     * 在系统关闭时，将MemTable中的数据写入磁盘
     * @Author Sjh
     * this is called when we need to manually start a merge process which force all data in memory to unstable file
     * on disk. we than create a new empty MemTable.
     * note that this method blocks current thread until all operation done.
     */
    @Override
    public void flushMemTable2Disk(){
        try{
            File tempFile = new File( this.dbDir + "/" + Filename.tempFileName( 0 ));
            if( !tempFile.exists() )
                tempFile.createNewFile();
            FileOutputStream outputStream = new FileOutputStream( tempFile );
            FileChannel channel = outputStream.getChannel();
            TableBuilder builer = new TableBuilder( new Options(), channel, TableComparator.instance() );
            SeekingIterator<Slice,Slice> iterator = this.memTable.iterator();
            while( iterator.hasNext() ){
                Map.Entry<Slice,Slice> entry = iterator.next();
                builer.add( entry.getKey(), entry.getValue() );
            }
            builer.finish();
            channel.close();
            outputStream.close();
        }catch( IOException e ){
            e.printStackTrace();
            throw new TPSRuntimeException("memTable flush failed", e);
        }
    }

    @Override
    public void flushMetaInfo2Disk(){
        try {
            SystemMetaController.forceToDisk(this.dbDir, this.meta);
        } catch (IOException e) {
            e.printStackTrace();
            throw new TPSRuntimeException("meta flush to disk failed", e);
        }
    }


}
