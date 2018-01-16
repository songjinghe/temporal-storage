package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.io.DataFileMetaInfo;


import java.util.*;

/**
 * Created by song on 17-12-19.
 */
public class TPMetaInfo
{
    private final int propertyId;
    private final TemporalPropertyType type = TemporalPropertyType.INVALID;
    private final TemporalValue fileTimeIndex;

    private long nextStableId;

    private final TreeMap<TimePoint, DataFileMetaInfo> stableFiles = new TreeMap<>();
    private final TreeMap<Long, DataFileMetaInfo> unStableFiles = new TreeMap<>();
    private final TreeMap<Long, DataFileMetaInfo> memLogs = new TreeMap<>();

    public TPMetaInfo(int propertyId)
    {
        this.fileTimeIndex = new TemporalValue(propertyId, -1);
        this.propertyId = propertyId;
    }

    public DataFileMetaInfo latestMemLog()
    {
        Map.Entry<Long, DataFileMetaInfo> e = memLogs.lastEntry();
        if(e==null)
        {
            return null;
        }else{
            return e.getValue();
        }
    }

    public List<DataFileMetaInfo> getMemLogToMerge()
    {
        Map.Entry<Long, DataFileMetaInfo> last = memLogs.lastEntry();
        if(last!=null){
            NavigableMap<Long, DataFileMetaInfo> pre = memLogs.headMap(last.getKey(), false);
            List<DataFileMetaInfo> result = new ArrayList<>();
            for(Map.Entry<Long, DataFileMetaInfo> e : pre.entrySet()){
                result.add(e.getValue());
            }
            return result;
        }else{
            return Collections.emptyList();
        }
    }

    public boolean needMergeMemLogFiles()
    {
        return memLogs.size()>2;
    }

    public List<DataFileMetaInfo> getUnStableToMerge()
    {
        List<DataFileMetaInfo> result = new ArrayList<>();
        for(int i=0; i<6; i++){
            DataFileMetaInfo usFile = unStableFiles.get(i);
            if(usFile!=null) result.add(usFile);
            else return result;
        }
        Collections.reverse(result);
        return result;
    }

    public DataFileMetaInfo getLatestStable()
    {
        Map.Entry<TimePoint, DataFileMetaInfo> last = stableFiles.lastEntry();
        return last==null ? null : last.getValue();
    }

    public void addStable(DataFileMetaInfo target)
    {
        stableFiles.put(target.getId(), target);
    }

    public void addUnStable(DataFileMetaInfo target)
    {
        unStableFiles.put(target.getId(), target);
    }

    public void addMemLog(DataFileMetaInfo fileMetaInfo)
    {
        memLogs.put(System.currentTimeMillis(), fileMetaInfo);
    }

    public void removeMemLog(DataFileMetaInfo fileMetaInfo)
    {
        memLogs.remove(fileMetaInfo);
    }

    public void removeUnStable(DataFileMetaInfo fileMetaInfo)
    {

    }

    public TimePoint stableMaxTime(){

    }

    public int getPropertyId()
    {
        return propertyId;
    }

    public long getNextStableId()
    {
        return nextStableId++;
    }
}
