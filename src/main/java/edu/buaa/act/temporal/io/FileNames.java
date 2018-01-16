package edu.buaa.act.temporal.io;

import com.google.common.collect.Maps;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created by song on 17-12-25.
 */
public class FileNames
{
    public static String memtableDump(){
        return "MEMORY_TABLE_DUMP";
    }

    public static Map<Integer, File> memLogFiles(File[] files)
    {
        if(files==null){
            return Collections.emptyMap();
        }else{
            Map<Integer, File> result = new TreeMap<>();
            for(File f : files)
            {
                String[] arr = f.getName().split("\\.");
                if(arr.length==2 && arr[1].toLowerCase().equals(".memlog") && arr[0].matches("\\d{3}")){
                    result.put(Integer.parseInt(arr[0]), f);
                }
            }
            return result;
        }
    }

    public static Entry<Integer, File> maxIdFile(Map<Integer, File> fileMap){
        if(fileMap.isEmpty()){
            return null;
        }else
        {
            List<Integer> ids = new ArrayList<>(fileMap.keySet());
            Collections.sort(ids);
            Integer maxId = ids.get(ids.size() - 1);
            return Maps.immutableEntry(maxId, fileMap.get(maxId));
        }
    }

    public static File getPath(File dbDir, DataFileMetaInfo metaInfo)
    {
        return null;
    }
}
