package org.act.temporalProperty.impl.index;

import com.google.common.primitives.Ints;
import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.TemporalPropertyStoreFactory;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.index.IndexQueryRegion;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.PropertyValueInterval;
import org.act.temporalProperty.util.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Created by song on 2018-01-22.
 */
public class BuildAndQueryTest {
    private static Logger log = LoggerFactory.getLogger(BuildAndQueryTest.class);

    private static File dataPath = new File("/home/song/tmp/road data");
    private static File dbDir = new File("/tmp/temporal.property.test");
    private static Map<String, Long> roadIdMap = new HashMap<>();
    private static TemporalPropertyStore store;

    @BeforeClass
    public static void initDB() throws Throwable {
        deleteAllFilesOfDir(dbDir);
        if(!dbDir.exists()) {
            dbDir.mkdir();
            store = TemporalPropertyStoreFactory.newPropertyStore(dbDir.getAbsolutePath());
            inputData(store);
//            testRangeQuery(store);
        }else{
            store = TemporalPropertyStoreFactory.newPropertyStore(dbDir.getAbsolutePath());
        }
    }

    @Before
    public void buildIndex(){
        List<Integer> proIds = new ArrayList<>();
        proIds.add(1);
        List<IndexValueType> types = new ArrayList<>();
        types.add(IndexValueType.INT);
//        store.createValueIndex(1288803660, 1288824660, proIds, types);
//        store.createValueIndex(1288800300, 1288802460, proIds, types);
        store.createValueIndex(1560, 27360, proIds, types);
        log.info("create index done");
    }

    @Test
    public void main() throws Throwable {
        IndexQueryRegion condition = new IndexQueryRegion(18300, 27000);
        Slice minValue = new Slice(4);
        minValue.setInt(0, 0);
        Slice maxValue = new Slice(4);
        maxValue.setInt(0, 200);
        condition.add(new PropertyValueInterval(1, minValue, maxValue, IndexValueType.INT));
        List<Long> result = store.getEntities(condition);
        log.info("{}", result.size());
//        for(Long eid : result){
//
//        }
    }

    @After
    public void closeDB(){
        if(store!=null) store.shutDown();
    }

    private static void inputData(TemporalPropertyStore store) throws IOException {
        List<File> dataFileList = new ArrayList<>();
        getFileRecursive(dataPath, dataFileList, 5);
        dataFileList.sort((Comparator.comparing(File::getName)));

        for (int i = 0; i < dataFileList.size() && i<100; i++) {
            File file = dataFileList.get(i);
            int time = timeStr2int(file.getName().substring(9, 21)) - 1288800000;
            try(BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                for (int lineCount = 0; (line = br.readLine()) != null; lineCount++) {
                    if (lineCount == 0) continue;
                    input(time, line, store);
                }
            }
//            log.info("input files done {} {}", file.getName(), time);
        }
        log.info("input files done");
    }

    private static void testRangeQuery(TemporalPropertyStore store) {
        store.getRangeValue(2, 2, 1560, 27000, new RangeQueryCallBack() {
            public void setValueType(String valueType) {}
            public void onCall(int time, Slice value) {
                log.info("{} {}", time, value.getInt(0));
            }
            public void onCallBatch(Slice batchValue){}
            public Object onReturn(){return null;}
            public CallBackType getType(){return null;}
        });
    }

    private static void getFileRecursive(File dir, List<File> fileList, int level){
        if(dir.isDirectory()){
            for (File file : dir.listFiles()) {
                if(!file.isDirectory() && file.getName().startsWith("TJamData_201") && file.getName().endsWith(".csv")){
                    fileList.add(file);
                }
            }
            if(level>0) {
                for (File file : dir.listFiles()) {
                    if (file.isDirectory()) {
                        getFileRecursive(file, fileList, level - 1);
                    }
                }
            }
        }
    }

    private static void input(int time, String line, TemporalPropertyStore store) {
        String[] fields = line.split(",");
//        int index = Integer.valueOf(fields[0]);
        String gridId = fields[1];
        String chainId = fields[2];
//        int ignore = Integer.valueOf(fields[3]);
//        type = Integer.valueOf(fields[4]);
//        length = Integer.valueOf(fields[5]);
        int travelTime = Integer.valueOf(fields[6]);
        int fullStatus = Integer.valueOf(fields[7]);
        int vehicleCount = Integer.valueOf(fields[8]);
        int segmentCount = Integer.valueOf(fields[9]);

        long roadId = getId(gridId, chainId);
//        log.debug("eid({}), time({}), travelTime({})", roadId, time, travelTime);
        setIntProperty(store, time, roadId, 1, travelTime);
        setIntProperty(store, time, roadId, 2, fullStatus);
        setIntProperty(store, time, roadId, 3, vehicleCount);
        setIntProperty(store, time, roadId, 4, segmentCount);
    }

    private static void setIntProperty(TemporalPropertyStore store, int time, long roadId, int propertyId, int value) {
        Slice id = getIdSlice(roadId, propertyId);
        InternalKey key = new InternalKey(id, time, 4, ValueType.VALUE);
        store.setProperty(key.encode(), ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
    }

    private static Slice getIdSlice(long roadId, int propertyId) {
        Slice result = new Slice(12);
        result.setLong(0, roadId);
        result.setInt(8, propertyId);
        return result;
    }

    private static long getId(String gridId, String chainId) {
        String strKey = gridId+":"+chainId;
        if(roadIdMap.containsKey(strKey)){
            return roadIdMap.get(strKey);
        }else{
            long nextId = roadIdMap.size();
            roadIdMap.put(strKey, nextId);
            return nextId;
        }
    }

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

    private static void deleteAllFilesOfDir(File path) {
        if (!path.exists())
            return;
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (int i = 0; i < files.length; i++) {
            deleteAllFilesOfDir(files[i]);
        }
        path.delete();
    }
}
