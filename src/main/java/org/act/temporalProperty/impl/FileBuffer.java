package org.act.temporalProperty.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.act.temporalProperty.impl.MemTable.MemTableIterator;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.table.UnSortedTable;
import org.act.temporalProperty.util.Slice;

/**
 * 是系统处理对StableFile或UnStableFile插入操作的机制，对应设计文档中的Buffer,包括了内存中的结果和对应的备份文件。每一个FileBuffer固定对应一个StableFile或UnStableFile。
 *
 */
public class FileBuffer implements Closeable
{
	/**
	 * 内存中保存数据
	 */
    private MemTable memTable;
    /**
     * 对应磁盘中的备份文件
     */
    private UnSortedTable discTable;
    
    private String fName;
    /**
     * 实例化方法
     * @param unSortedTableFile 在磁盘中起备份作用的文件的绝对名称
     * @throws IOException
     */
    public FileBuffer(File unSortedTableFile ) throws IOException
    {
        this.fName = unSortedTableFile.getAbsolutePath();
        this.memTable = new MemTable( TableComparator.instance() );
        if( !unSortedTableFile.exists() ){
            Files.createFile(unSortedTableFile.toPath());
            this.discTable = new UnSortedTable(unSortedTableFile);
        }else{
            this.discTable = new UnSortedTable(unSortedTableFile);
            this.discTable.initFromFile( this.memTable );
        }
    }
    
    /**
     * 向Buffer中插入一个数据项
     * @param key 数据项的key
     * @param value 值
     * @throws IOException
     */
    public void add( Slice key, Slice value ) throws IOException
    {
        discTable.add( key, value );
        this.memTable.add( key, value );
    }

    public void force() throws IOException
    {
        discTable.addCheckPoint();
    }
    
    /**
     * 返回用于访问的iterator
     * @return
     */
    public MemTableIterator iterator()
    {
        return this.memTable.iterator();
    }

    /**
     * 当Buffer参与任何合并过程之后，需要将其删除。在删除之前需调用close()方法。
     */
    @Override
    public void close() throws IOException
    {
        this.memTable = null;
        this.discTable.close();
    }

    public long size(){
        return this.memTable.approximateMemUsage();
    }

    @Override
    public String toString() {
        return "FileBuffer{"+fName+","+size()+"}";
    }
}
