package org.act.temporalProperty.impl;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.index.*;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.table.MergeProcess;
import org.act.temporalProperty.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * DynPropertyStore的实现类
 *
 */
public class TemporalPropertyStoreImpl implements TemporalPropertyStore
{
    private UnstableLevel unLevel;
    private StableLevel stlevel;
    private MergeProcess mergeProcess;
    private String dbDir;
    private TemporalValueIndex index;
    private ReadWriteLock fileMetaLock;
    private Logger log = LoggerFactory.getLogger( TemporalPropertyStoreImpl.class );
    
    /**
     * 实例化方法
     * @param dbDir 存储动态属性数据的目录地址
     */
    public TemporalPropertyStoreImpl(String dbDir ) throws Throwable
    {
        this.dbDir = dbDir;
        this.fileMetaLock = new ReentrantReadWriteLock( true );
        this.stlevel = new StableLevel( dbDir,fileMetaLock );
        this.mergeProcess = new MergeProcess( dbDir, this.stlevel,fileMetaLock );
        this.unLevel = new UnstableLevel( dbDir, this.mergeProcess,fileMetaLock, this.stlevel );
        this.mergeProcess.setUnStableLevel(this.unLevel);
//        Runtime.getRuntime().addShutdownHook( new Thread(){
//            public void run()
//            {
//                TemporalPropertyStoreImpl.this.stop();
//            }
//        });
        start();
    }
    
    
    public void shutDown()
    {
    	this.stop();
    }
    
    /**
     * 退出系统时调用，主要作用是将内存中的数据写入磁盘。
     */
    private void stop()
    {
        this.unLevel.dumpMemTable2disc();
        this.unLevel.dumpFileMeta2disc();
        this.stlevel.dumpFileMeta2disc();
    }
    
    /**
     * 系统启动时调用，主要作用是将上次系统关闭时写入磁盘的数据读入内存
     */
    private void start() throws Throwable
    {
        List<FileMetaData> unstableFiles = readMetaInfo("unstable");
        List<FileMetaData> stableFiles = readMetaInfo("stable");

        for(FileMetaData file : unstableFiles){
            this.unLevel.initFromDisk( file );
        }

        for(FileMetaData file: stableFiles){
            this.stlevel.initFromDisk( file );
        }

        Files.deleteIfExists(new File(this.dbDir+'/'+Filename.tempFileName(6)).toPath());
        Files.deleteIfExists(new File(this.dbDir+'/'+Filename.tempFileName(7)).toPath());

        this.unLevel.restoreMemTable();
    }

    private List<FileMetaData> readMetaInfo(String stableOrUnStable) throws IOException {
        List<FileMetaData> metaDataList = new ArrayList<>();
        File metaFile = getValidMetaFile(stableOrUnStable);
        if( metaFile.exists() )
        {
            FileInputStream inputStream = new FileInputStream( metaFile );
            FileChannel channel = inputStream.getChannel();
            LogReader logReader = new LogReader( channel, null, false, 0 );
            for( Slice logRecord = logReader.readRecord(); logRecord != null; logRecord = logReader.readRecord() )
            {
                VersionEdit edit = new VersionEdit( logRecord );
                for( Entry<Integer,FileMetaData> entry : edit.getNewFiles().entries() )
                {
                    metaDataList.add(entry.getValue());
                }
            }
            inputStream.close();
            channel.close();
        }
        return metaDataList;
    }

    private File getValidMetaFile(String stableOrUnStable) throws IOException {
        File oldMetaFile = new File( this.dbDir + "/"+stableOrUnStable+".meta" );
        File newMetaFile = new File( this.dbDir + "/"+stableOrUnStable+".new.meta" );
        if( oldMetaFile.exists()) { // old exist then use old.
            if(newMetaFile.exists() && !newMetaFile.delete()){ // delete new.
                throw new IOException("Recovery: can not delete " + stableOrUnStable + ".new.meta file!");
            }
            return oldMetaFile;
        }else if(newMetaFile.exists()){
            if(isValidMetaFile(newMetaFile)) {
                // old not exist and new exist & valid. so rename to old
                if (!newMetaFile.renameTo(oldMetaFile)) {
                    throw new IOException("Recovery: can not rename " + stableOrUnStable + ".new.meta file!");
                }
            }else{
                // old not exist and new invalid. so db is damaged.
                throw new IOException("Recovery: invalid " + stableOrUnStable + ".new.meta , db is damaged!");
            }
            return oldMetaFile;
        }else { // both old new not exist. is a empty folder. do nothing.
            return oldMetaFile;
        }
    }

    private boolean isValidMetaFile(File newMetaFile) throws IOException {
        String EOF = "EOF!EOF!EOF!";
        long index = newMetaFile.length() - EOF.length();
        if( index > 0 ) {
            RandomAccessFile raf = new RandomAccessFile(newMetaFile, "r");
            raf.seek(index);// Seek to the end of file
            byte[] bytes = new byte[EOF.length()];
            raf.read(bytes, 0, EOF.length());// Read it out.
            raf.close();
            return new String(bytes).equals(EOF);
        }else{
            return false;
        }
    }


    /**
     * 进行时间点查询，参考{@link TemporalPropertyStore}中的说明
     */
    @Override
    public Slice getPointValue( long id, int proId, int time )
    {
        Slice idSlice = new Slice( 12 );
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );
        if( time >= this.stlevel.getTimeBoundary() )
        {
            Slice toret = this.unLevel.getPointValue( idSlice, time );
            if( null == toret )
                return this.stlevel.getPointValue( idSlice, time );
            else if( toret.length() == 0 )
                return null;
            else
                return toret;
        }
        else
            return this.stlevel.getPointValue( idSlice, time );
    }
    
    /**
     * 进行实践段查询，参考{@link TemporalPropertyStore}中的说明
     */
    @Override
    public Object getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        this.fileMetaLock.readLock().lock();
        Slice idSlice = new Slice( 12 );
        idSlice.setLong( 0, id );
        idSlice.setInt( 8, proId );

        EPAppendIterator diskFileIter = new EPAppendIterator(idSlice);
        if( startTime < this.stlevel.getTimeBoundary() ) {
            diskFileIter.append(this.stlevel.getRangeValueIter(idSlice, startTime, Math.min((int) this.stlevel.getTimeBoundary(), endTime)));
        }
        if( endTime >= this.stlevel.getTimeBoundary() ) {
            diskFileIter.append(this.unLevel.getRangeValueIter(idSlice, Math.max((int) this.stlevel.getTimeBoundary(), startTime), endTime));
        }

        SeekingIterator<Slice,Slice> mergedIterator;
        if(diskFileIter.size()>0){
            mergedIterator = new EPMergeIterator(idSlice, diskFileIter, this.unLevel.getMemTableIter(idSlice));
        }else{
            mergedIterator = this.unLevel.getMemTableIter(idSlice);
        }

        InternalKey searchKey = new InternalKey( idSlice, startTime, 0, ValueType.VALUE );
        mergedIterator.seek(searchKey.encode());
        while(mergedIterator.hasNext()){
            Entry<Slice,Slice> entry = mergedIterator.next();
            InternalKey key = new InternalKey( entry.getKey() );
            if( key.getStartTime() <= endTime && key.getValueType() == ValueType.VALUE){
                callback.onCall(key.getStartTime(), entry.getValue());
            }else break;
        }
        this.fileMetaLock.readLock().unlock();
        return callback.onReturn();
    }

    
    /**
     * 写数据，参考{@link TemporalPropertyStore}中的说明
     */
    @Override
    public boolean setProperty( Slice key, byte[] value )
    {
        Slice valueSlice = new Slice( value );
        InternalKey internalKey = new InternalKey( key );
        return this.unLevel.set( internalKey, valueSlice );
    }

    @Override
    public boolean delete( Slice id )
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types) {
        unLevel.createValueIndex(start, end, proIds, types);
    }

    @Override
    public List<Long> getEntities(IndexQueryRegion condition) {
        List<IndexEntry> result = getEntries(condition);
        Set<Long> set = new HashSet<>();
        for(IndexEntry entry : result){
            set.add(entry.getEntityId());
        }
        return new ArrayList<>(set);
    }

    @Override
    public List<IndexEntry> getEntries(IndexQueryRegion condition) {
        try {
            return unLevel.valueIndexQuery(condition);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    /**
     * @Author Sjh
     * this is called when we need to manually start a merge process which force all data in memory to unstable file
     * on disk. we than create a new empty MemTable.
     * note that this method blocks current thread until all operation done.
     */
    @Override
    public void flushMemTable2Disk()
    {
        this.unLevel.forceMemTableMerge();
    }


    @Override
    public void flushMetaInfo2Disk()
    {
        this.unLevel.forceFileMetaToDisk();
        this.stlevel.dumpFileMeta2disc();
    }


}
