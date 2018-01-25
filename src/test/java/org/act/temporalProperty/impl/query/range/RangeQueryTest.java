package org.act.temporalProperty.impl.query.range;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.StoreBuilder;
import org.act.temporalProperty.util.TrafficDataImporter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by song on 2018-01-23.
 */
public class RangeQueryTest {
    private static Logger log = LoggerFactory.getLogger(RangeQueryTest.class);
    private static String dataPath = "/home/song/tmp/road data";
    private static String dbDir = "/tmp/temporal.property.test";
    private TemporalPropertyStore store;
    private TrafficDataImporter importer;

    @Test
    public void test2fail() throws Throwable {
        StoreBuilder stBuilder = new StoreBuilder(dbDir, false);
        this.store = stBuilder.store();
//        for(int version=1; version<5; version++){
//            for(int time=0; time<Integer.MAX_VALUE/1000-6; time+=1){
//                StoreBuilder.setIntProperty(store, time, 0, 0, version);
//            }
//            log.debug("version {} write finish", version);
//        }
        EntityIdCallBack callback = new EntityIdCallBack(0, Integer.MAX_VALUE / 1000 - 6);
        this.store.getRangeValue(0,0,0,Integer.MAX_VALUE/1000-6, callback);
        log.info("onCall called {} times", callback.i);
        store.shutDown();
    }

    @Test
    public void main() throws Throwable {
        StoreBuilder stBuilder = new StoreBuilder(dbDir, true);
        this.store = stBuilder.store();
        importer = new TrafficDataImporter(store, dataPath, 100);
        int timeMin = importer.getMinTime();
        int timeMax = importer.getMaxTime();
        log.info("time: {} - {}", timeMin, timeMax);
        EntityIdCallBack callback = new EntityIdCallBack(timeMin, timeMax);
        store.getRangeValue(1, 1, timeMin, timeMax, callback);
        List<Integer> list = callback.getTimeList();
        for(Integer time : list){
            log.info("{}", time);
        }
        this.store.shutDown();
    }

    private class EntityIdCallBack extends RangeQueryCallBack {
        private int timeMin, timeMax, lastTime = -1;
        private boolean first = true;
        private List<Integer> timeList = new ArrayList<>();
        private int i = 0;
        private StringBuilder stringBuilder = new StringBuilder();

        public EntityIdCallBack(int timeMin, int timeMax) {
            this.timeMin = timeMin;
            this.timeMax = timeMax;
        }

        public List<Integer> getTimeList(){
            return timeList;
        }

        private boolean overlap(int t1min, int t1max, int t2min, int t2max){
            return (t1min<=t2max && t2min<=t1max);
        }

        public void onCall(int time, Slice value) {
            this.timeList.add(time);
            int val = value.getInt(0);
            i++;
//            stringBuilder.append(String.format("(%07d,%d) ", time, val));
//            if(i%16==0) stringBuilder.append('\n');
            if(val!=4){
                log.info("value not latest, get "+ val+" at time "+ time);
            }
            if(first){
                first=false;
            }else{
                if(lastTime>=time){
                    log.info("time not inc: last("+lastTime+") cur("+time+")");
                }
//                if(overlap(lastTime, time, timeMin, timeMax)){
//                    log.info("time ")
//                }
            }
            lastTime = time;
        }
        public void setValueType(String valueType) {
            log.info("setValueType called");
        }
        public void onCallBatch(Slice batchValue) {
            log.info("onCallBatch called");
        }
        public Object onReturn() {
            log.info("onReturn called");
            return null;
        }
        public RangeQueryCallBack.CallBackType getType() {
            log.info("getType called");
            return RangeQueryCallBack.CallBackType.USER;
        }
    }
}
