package edu.buaa.act.temporal.impl;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import edu.buaa.act.temporal.impl.index.TPIndex;
import edu.buaa.act.temporal.io.DataFileMetaInfo;
import org.act.temporalProperty.impl.FileMetaData;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by song on 17-12-6.
 */
public class DBMetaInfo
{

    private final File file;



    private long lastUpdateTime = System.currentTimeMillis();

    private SortedMap<Integer, FileMetaData> unStableFiles = new TreeMap<>();
    private SortedMap<Integer, FileMetaData> stableFiles = new TreeMap<>();
    private Map<Integer, TemporalPropertyController> tpMap = new HashMap<>();
    private Map<Integer, TPIndex> indexMap = new HashMap<>();
    Table<Integer, Long, DataFileMetaInfo> infoTable = HashBasedTable.create();

    public DBMetaInfo(File dbMetaFile)
    {
        this.file = dbMetaFile;
    }

    public void remove(DataFileMetaInfo info)
    {
        infoTable.remove(info.getPropertyId(), info.getId());
    }

    public void add(DataFileMetaInfo info)
    {
        infoTable.put(info.getPropertyId(), info.getId(), info);
    }

    public SortedMap<Integer, FileMetaData> getUnStableFiles()
    {
        return unStableFiles;
    }

    public SortedMap<Integer, FileMetaData> getStableFiles()
    {
        return stableFiles;
    }




    public TemporalPropertyController getTPDisk(Integer propertyId)
    {
        return null;
    }
}
