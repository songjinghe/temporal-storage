package org.act.temporalProperty.meta;

import org.act.temporalProperty.impl.FileMetaData;
import org.act.temporalProperty.util.Slice;

import java.util.TreeMap;

/**
 * Created by song on 2018-01-17.
 */
public class PropertyMetaData {
    private final int propertyId;
    private final ValueContentType type;
    private final TemporalValue<FileMetaData> fileTimeIndex = new TemporalValue<>();

    private long nextStableId;

    private final TreeMap<Long, FileMetaData> stableFiles = new TreeMap<>();
    private final TreeMap<Long, FileMetaData> unStableFiles = new TreeMap<>();
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


    public void addUnstable(FileMetaData file) {
        unStableFiles.put(file.getNumber(), file);
    }

    public void addStable(FileMetaData file) {
        stableFiles.put(file.getNumber(), file);
    }
}
