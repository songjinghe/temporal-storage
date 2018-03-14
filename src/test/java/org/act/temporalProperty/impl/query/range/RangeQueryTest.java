package org.act.temporalProperty.impl.query.range;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.StoreBuilder;
import org.act.temporalProperty.util.TrafficDataImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by song on 2018-01-23.
 */
public class RangeQueryTest {
    private static Logger log = LoggerFactory.getLogger(RangeQueryTest.class);
    private static String dataPath = "/home/song/tmp/road data";
    private static String dbDir = "/tmp/temporal.property.test";
    private TemporalPropertyStore store;
    private TrafficDataImporter importer;

    @Before
    public void initDB() throws Throwable {
        StoreBuilder stBuilder = new StoreBuilder(dbDir, true);
        importer = new TrafficDataImporter(stBuilder.store(), dataPath, 100);
        log.info("time: {} - {}", importer.getMinTime(), importer.getMaxTime());
        store = stBuilder.store();
    }

    @Test
    public void test2fail() throws Throwable {
        this.store.getRangeValue(62254,1,0,Integer.MAX_VALUE-6, new CustomCallBack(62254){
            public void onCall(int time, Slice value) { log.info("{} {}", time, value.getInt(0));}
            public Object onReturn() { log.info("onReturn of test"); return null;}
        });
        log.info("=================");
        this.store.getRangeValue(62254,1,18360,18959, new CustomCallBack(62254){
            public void onCall(int time, Slice value) { log.info("{} {}", time, value.getInt(0));}
            public Object onReturn() { log.info("onReturn of test"); return null;}
        });
    }

    private class CustomCallBack extends RangeQueryCallBack {
        private long entityId;
        public CustomCallBack(long entityId){this.entityId=entityId;}
        public void onCall(int time, Slice value) {}
        public void setValueType(String valueType) {}
        public void onCallBatch(Slice batchValue) {}
        public Object onReturn() {return null;}
        public CallBackType getType() {return null;}
    }

    private class StopLoopException extends RuntimeException {
        public StopLoopException() {}
        public StopLoopException(String s) {
            super(s);
        }
    }

    @After
    public void closeDB(){
        if(store!=null) store.shutDown();
    }

}
