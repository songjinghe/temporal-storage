package org.act.temporalProperty.impl.index;

import org.act.temporalProperty.index.rtree.IndexEntry;

import java.io.*;
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

    // the files filtered by (startTime, endTime)
    private List<File> dataFileList;

    private Map<Integer, ArrayList<IndexEntry>> entryMap;

    public SourceCompare(String dataPath) {
        this.dataDir = new File(dataPath);
        this.dataFileList = new ArrayList<File>();
        this.entryMap = new HashMap<Integer, ArrayList<IndexEntry>>();
    }

    // According to the (startTime, endTime; inputFileCount) to filter files in dataDir
    public int importFiles(int inputFileCount, int starTime, int endTime) {
        List<File> totalFileList = new ArrayList<File>();

        if(!this.dataDir.isDirectory()) {
            System.out.println("SoureCompare-importFiles: dataDir = %s is not a Directory.");
            return -1;
        }

        for (File file : this.dataDir.listFiles()) {
            if (file.isFile() && file.getName().startsWith("TJamData_201") && file.getName().endsWith(".csv")) {
                totalFileList.add(file);
            }
        }
        totalFileList.sort(Comparator.comparing(File::getName));

        for (int i = 0; (i < totalFileList.size()) && (i < inputFileCount); i++) {
            File file = totalFileList.get(i);
            int time = timeStr2int(file.getName().substring(9, 21)) - 1288800000;

            if ((time >= starTime) && (time < endTime)) {
                this.dataFileList.add(file);
            }
        }

        return 0;
    }

    /* Read (startTime, endTime, [propertyId]; entityId, propertyValue) into a array (iterator); List<List<entry>()> --- each file one list;
    *  use propertyValue to filter; HashMap<travelTime, EntryList>
    */
    public int importEntries() {

        for (File file : this.dataFileList) {

           // FileReader fr = new FileReader(file);

            try (BufferedReader br = new BufferedReader(new FileReader(file.getName()))) {

                String line = br.readLine();
                while((line = br.readLine()) != null) {

                }
            } catch(IOException e) {
                System.out.println("FileReader: NullIOException.");
            }
        }
        return 0;
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
