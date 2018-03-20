package org.act.temporalProperty.meta;

import com.google.common.collect.Lists;
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

    private int memTableMinTime;

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

    public int memMinTime() {
        return memTableMinTime;
    }

    public void updateMemTableMinTime(int memTableMinTime) {
        if(memTableMinTime > this.memTableMinTime) {
            this.memTableMinTime = memTableMinTime;
        }
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
    public List<FileMetaData> overlappedUnstable(int startTime, int endTime) {
        Integer start = unstableByTime.floorKey(startTime);
        if(start!=null) {
            startTime = start;
        }
        return new ArrayList<>(unstableByTime.subMap(startTime, true, endTime, true).values());
    }

    // returned meta's time is ASC order
    public List<FileMetaData> overlappedStable(int startTime, int endTime) {
        Integer start = stableByTime.floorKey(startTime);
        if(start!=null) {
            startTime = start;
        }
        return new ArrayList<>(stableByTime.subMap(startTime, true, endTime, true).values());
    }

    // time is ASC order
    public List<FileMetaData> unFloorTime(int time) {
        Integer start = unstableByTime.floorKey(time);
        if(start==null) {
            return null;
        }else{
            return new ArrayList<>(unstableByTime.headMap(start, true).values());
        }
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
    /**
     * 返回StableLevel存储的数据的最晚有效时间
     * @return -1 if no stable file available.
     */
    public int stMaxTime(){
        long lastNumber;
        try{
            lastNumber = stableFiles.lastKey();
            return stableFiles.get( lastNumber ).getLargest();
        }catch( NoSuchElementException e ){
            return -1;
        }
    }

    // return the stable meta which contains the time.
    public FileMetaData stHasTime(int time) {
        Integer start = stableByTime.floorKey(time);
        if(start==null) {
            return null;
        }else{
            return unstableByTime.get(start);
        }
    }

    public int unMinTime() {
        try {
            return unstableByTime.firstKey();
        }catch (NoSuchElementException e) {
            return 0;
        }
    }

    public boolean hasStable(){
        return stableFiles.size()>0;
    }

    public boolean hasUnstable(){
        return unStableFiles.size()>0;
    }

    public int unMaxTime() {
        return 0;
    }

    public void addUnstableBuffer(long number, FileBuffer buffer) {
        unStableFileBuffers.put(number, buffer);
    }

    public void addStableBuffer(long number, FileBuffer buffer) {
        stableFileBuffers.put(number, buffer);
    }


}
