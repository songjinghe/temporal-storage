package org.act.temporalProperty.impl.index;

import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.util.Slice;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
/*
* edit by yf
* edit from 2018-03-13
*
* 1. According to the (startTime, endTime; inputFileCount) to filter files in dataDir
* 2. Read (startTime, endTime, propertyId; entityId, propertyValue) into a array (iterator); List<List<entry>()> --- each file one list
* 3. use <propertyValue, entry> as <K, V> build B+ tree (support range search), to reduce the size of B+ tree, each file one tree
* 4. search the trees according to (valMin, valMax), store the result to a list
* 5. compare the result with queryByRange and queryByIndex
*
* => for step 2.3.4, change B+ tree to HashMap<K, List>, then traverse the Map O(n) ; if the time is too long, change it to the B+ tree
*    Notice: HashMap only support a single thread, for concurrent situation, it should be changed to HashTable
*/

public class SourceCompare {

    // traffic data dir path
    private final File dataDir;

    // ---%--the files filtered by (startTime, endTime)
    //private List<File> dataFileList;
    private List<File> totalFileList;

    private Map<String, Long> entityIdMap;

    //private Map<Integer, ArrayList<IndexEntry>> entryMap;

    private final int inputFileCount;

    public SourceCompare(String dataPath, int inputFileCount) {
        this.dataDir = new File(dataPath);
        //this.dataFileList = new ArrayList<File>();
        //this.entryMap = new HashMap<Integer, ArrayList<IndexEntry>>();
        this.entityIdMap = new HashMap<String, Long>();
        this.totalFileList = new ArrayList<File>();
        this.inputFileCount = inputFileCount;
    }

    public List<IndexEntry> queryBySource(int timeMin, int timeMax, int valueMin, int valueMax) {

        int ret1 = importFiles();
        if(ret1 < 0)
            return null;

        return filterEntry(timeMin, timeMax, valueMin, valueMax);
    }

    // According to the (startTime, endTime; inputFileCount) to filter files in dataDir
    /* Read (startTime, endTime, [propertyId]; entityId, propertyValue) into a array (iterator); List<List<entry>()> --- each file one list;
     *  use propertyValue to filter; HashMap<travelTime, EntryList>
     */

    private int importFiles() {

        if (!this.dataDir.isDirectory()) {
            System.out.println("SoureCompare-importFiles: dataDir = %s is not a Directory.");
            return -1;
        }

        for (File file : this.dataDir.listFiles()) {
            if (file.isFile() && file.getName().startsWith("TJamData_201") && file.getName().endsWith(".csv")) {
                totalFileList.add(file);
            }
        }
        totalFileList.sort(Comparator.comparing(File::getName));

        return 0;
    }

    private List<IndexEntry> filterEntry(int starTime, int endTime, int valueMin, int valueMax){

        int fileCount = Math.min(totalFileList.size(), inputFileCount);
        List<IndexEntry> entryList = new ArrayList<IndexEntry>();

        for (int i = 0; i < fileCount; i++) {
            File file = totalFileList.get(i);
            int sTime = timeStr2int(file.getName().substring(9, 21)) - 1288800000;

            try (BufferedReader br = new BufferedReader(new FileReader(file))) {

                String line = br.readLine();
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split(",");

                    String gridId = fields[1];
                    String chainId = fields[2];
                    Long entityId = getEntityId(gridId, chainId);

                    int travelTime = Integer.valueOf(fields[6]);

                    if((sTime >= starTime) && (sTime < endTime) && (travelTime >= valueMin) && (travelTime <= valueMax)) {

                        int eTime = getEndTime(i + 1, endTime);
                        Slice value = new Slice(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(travelTime).array());

                        entryList.add(new IndexEntry(entityId, sTime, eTime, new Slice[]{value}));
                    }

                }
            } catch (IOException e) {
                System.out.println("FileReader: NullIOException.");
            }
        }

        return entryList;
    }

    private int getEndTime(int fileNum, int endTime){
        if(fileNum < totalFileList.size()) {
            File file = totalFileList.get(fileNum);
            return (timeStr2int(file.getName().substring(9, 21)) - 1288800000);
        } else {
            return endTime;
        }
    }

    private Long getEntityId(String gridId, String chainId){

        String key = gridId + ":" + chainId;

        if(entityIdMap.containsKey(key)) {
            return entityIdMap.get(key);
        } else {
            long value = entityIdMap.size();
            entityIdMap.put(key, value);
            return value;
        }
    }

    /*
    the same function as TrafficDataImporter.java->timeStr2int()
     */
    private static int timeStr2int(String tStr){
        String yearStr = tStr.substring(0,4);
        String monthStr = tStr.substring(4,6);
        String dayStr = tStr.substring(6,8);
        String hourStr = tStr.substring(8,10);
        String minuteStr = tStr.substring(10, 12);
//        System.out.println(yearStr+" "+monthStr+" "+dayStr+" "+hourStr+" "+minuteStr);
        int year = Integer.parseInt(yearStr);
        int month = Integer.parseInt(monthStr)-1;//month count from 0 to 11, no 12
        int day = Integer.parseInt(dayStr);
        int hour = Integer.parseInt(hourStr);
        int minute = Integer.parseInt(minuteStr);
//        System.out.println(year+" "+month+" "+day+" "+hour+" "+minute);
        Calendar ca= Calendar.getInstance();
        ca.set(year, month, day, hour, minute, 0); //seconds set to 0
        long timestamp = ca.getTimeInMillis();
//        System.out.println(timestamp);
        if(timestamp/1000<Integer.MAX_VALUE){
            return (int) (timestamp/1000);
        }else {
            throw new RuntimeException("timestamp larger than Integer.MAX_VALUE, this should not happen");
        }
    }

}
