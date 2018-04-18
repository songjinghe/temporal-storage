package org.act.temporalProperty.util;

import org.act.temporalProperty.impl.index.multival.BuildAndQueryTest;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class DataFileImporter {
    private static Logger log = LoggerFactory.getLogger(BuildAndQueryTest.class);

    private List<File> dataFileList;
    private String dataPath;
    private String dbDir;
    private File dataDir;

    public DataFileImporter() {
        setDataPath();
        setDbDir();
        this.dataFileList = new ArrayList<>();
        this.dataDir = new File(dataPath);
        importDataFiles(dataDir);
        dataFileList.sort(Comparator.comparing(File::getName));
    }

    private void importDataFiles(File dataDir) {
        if (!dataDir.isDirectory()) return;

        for (File file : dataDir.listFiles()) {
            if (file.isFile() && file.getName().startsWith("TJamData_201") && file.getName().endsWith(".csv")) {
                dataFileList.add(file);
            } else if (file.isDirectory()) {
                importDataFiles(file);
            }
        }
    }

    private void setDataPath() {
        if(SystemUtils.IS_OS_WINDOWS){
//            this.dataPath = "C:\\Users\\Administrator\\Desktop\\TGraph-source\\20101104.tar\\20101104";
            dataPath = "D:\\songjh\\projects\\TGraph\\test-traffic-data\\20101105";
        }else{
            this.dataPath = "/home/song/tmp/road data/20101104";
        }
    }

    private void setDbDir() {
        if(SystemUtils.IS_OS_WINDOWS){
            this.dbDir = "temporal.property.test";
        }else{
            this.dbDir = "/tmp/temporal.property.test";
        }
    }

    public String getDataPath() {return this.dataPath; }

    public String getDbDir() {return this.dbDir; }

    public List<File> getDataFileList() {return dataFileList; }
}
