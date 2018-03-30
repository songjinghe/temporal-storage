package org.act.temporalProperty.meta;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.FileBuffer;
import org.act.temporalProperty.impl.FileMetaData;
import org.act.temporalProperty.util.Slice;

import java.util.*;
import java.util.Map.Entry;

/**
 * Created by song on 2018-01-17.
 */
public class PropertyMetaData {
    private final int propertyId;
    private final ValueContentType type;
    private final TreeMap<Integer, FileMetaData> unstableByTime = new TreeMap<>();
    private final TreeMap<Integer, FileMetaData> stableByTime = new TreeMap<>();
    //所有的StableFile的元信息
    private final TreeMap<Long, FileMetaData> stableFiles = new TreeMap<>();
    //所有StableFile对应的Buffer
    private final TreeMap<Long, FileBuffer> stableFileBuffers = new TreeMap<>();
    //所有的UnStableFile的元信息
    private final TreeMap<Long, FileMetaData> unStableFiles = new TreeMap<>();
    //所有UnStableFile对应的Buffer
    private final TreeMap<Long, FileBuffer> unStableFileBuffers = new TreeMap<>();
    //    private final TreeMap<Long, FileMetaData> memLogs = new TreeMap<>();

    public PropertyMetaData(int propertyId, ValueContentType type){
        this.propertyId = propertyId;
        this.type = type;
    }

    public Integer getPropertyId() {
        return propertyId;
    }

    public ValueContentType getType() {
        return type;
    }

    public TreeMap<Long, FileMetaData> getStableFiles() {
        return stableFiles;
    }

    public TreeMap<Long, FileMetaData> getUnStableFiles() {
        return unStableFiles;
    }

    public Slice encode() {
        return PropertyMetaDataController.encode(this);
    }

    public long nextStableId(){
        return stableFiles.size();
    }

    public void addUnstable(FileMetaData file) {
        unStableFiles.put(file.getNumber(), file);
        unstableByTime.put(file.getSmallest(), file);
    }

    public void addStable(FileMetaData file) {
        stableFiles.put(file.getNumber(), file);
        stableByTime.put(file.getSmallest(), file);
    }

    public FileMetaData latestStableMeta(){
        Entry<Integer, FileMetaData> entry = stableByTime.lastEntry();
        if(entry!=null) return entry.getValue();
        else return null;
    }

    public FileBuffer getUnstableBuffers(long number) {
        return unStableFileBuffers.get(number);
    }

    public FileBuffer getStableBuffers(long number) {
        return stableFileBuffers.get(number);
    }

    public SortedMap<Long, FileBuffer> getUnstableBuffers() {
        return unStableFileBuffers;
    }

    public SortedMap<Long, FileBuffer> getStableBuffers() {
        return stableFileBuffers;
    }

    public void delUnstable(Long fileNumber) {
        FileMetaData meta = unStableFiles.get(fileNumber);
        unstableByTime.remove(meta.getSmallest());
        unStableFiles.remove(meta.getNumber());
    }

    public void delUnstableBuffer(Long fileNumber) {
        unStableFileBuffers.remove(fileNumber);
    }

    public void delStableBuffer(long number) {
        stableFileBuffers.remove(number);
    }

    // returned meta's time is ASC order
    public List<FileMetaData> overlappedStable(int startTime, int endTime) {
        Integer start = stableByTime.floorKey(startTime);
        if(start!=null) {
            startTime = start;
        }
        return new ArrayList<>(stableByTime.subMap(startTime, true, endTime, true).values());
    }

    /**
     * Return unstable metas whose corresponding file needed to be searched when query value at given `time`
     * the returned meta list, time is ASC order
     */
    public List<FileMetaData> unFloorTime(int time) {
        return new ArrayList<>(unstableByTime.headMap(time, true).values());
    }

    public FileMetaData unFloorTimeOneMeta(int time) {
        Integer start = unstableByTime.floorKey(time);
        if(start==null) {
            return null;
        }else{
            return unstableByTime.get(start);
        }
    }

    public FileMetaData stFloorTimeOneMeta(int time) {
        Integer start = stableByTime.floorKey(time);
        if(start==null) {
            return null;
        }else{
            return stableByTime.get(start);
        }
    }

    public int diskFileMaxTime(){
        if(hasUnstable()) return unMaxTime();
        else if(hasStable()) return stMaxTime();
        else throw new TPSNHException("no disk file!");
    }

    /**
     * 返回StableLevel存储的数据的最晚有效时间
     * @return -1 if no stable file available.
     */
    public int stMaxTime(){
        if(hasStable()){
            return stableByTime.lastEntry().getValue().getLargest();
        }else{
            throw new TPSNHException("no stable file available!");
        }
    }

    public int unMaxTime() {
        if(hasUnstable()){
            return unstableByTime.lastEntry().getValue().getLargest();
        }else{
            throw new TPSNHException("no unstable files!");
        }
    }

    /**
     * @param time is usually larger than stMaxTime.
     * @return the stable meta which contains the time.
     */
    public FileMetaData getStContainsTime(int time) {
        assert hasStable():"no stable file!";
        assert 0<=time:"time should >0 !";
        Integer start = stableByTime.floorKey(time);
        Preconditions.checkNotNull(start, new TPSNHException("should have 0<=time but get null"));
        return stableByTime.get(start);
    }

    public boolean hasStable(){
        return !stableFiles.isEmpty();
    }

    public boolean hasUnstable(){
        return !unStableFiles.isEmpty();
    }

    public boolean hasDiskFile(){
        return hasUnstable() || hasStable();
    }

    public void addUnstableBuffer(long number, FileBuffer buffer) {
        unStableFileBuffers.put(number, buffer);
    }

    public void addStableBuffer(long number, FileBuffer buffer) {
        stableFileBuffers.put(number, buffer);
    }

    @Override
    public String toString() {
        return "PropertyMetaData{" +
                "propertyId=" + propertyId +
                ", type=" + type +
                ", stableFiles=" + stableFiles +
                ", stableFileBuffers=" + stableFileBuffers +
                ", unStableFiles=" + unStableFiles +
                ", unStableFileBuffers=" + unStableFileBuffers +
                '}';
    }
}
