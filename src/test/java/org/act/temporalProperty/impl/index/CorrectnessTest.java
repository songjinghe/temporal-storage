package org.act.temporalProperty.impl.index;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.index.IndexQueryRegion;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.PropertyValueInterval;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.StoreBuilder;
import org.act.temporalProperty.util.TrafficDataImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by song on 2018-01-27.
 */
public class CorrectnessTest {
    private static Logger log = LoggerFactory.getLogger(BuildAndQueryTest.class);

    private static String dataPath = "/home/song/tmp/road data";
    private static String dbDir = "/tmp/temporal.property.test";

    private TemporalPropertyStore store;
    private StoreBuilder stBuilder;
    private TrafficDataImporter importer;

    @Before
    public void initDB() throws Throwable {
        stBuilder = new StoreBuilder(dbDir, true);
        importer = new TrafficDataImporter(stBuilder.store(), dataPath, 100);
        log.info("time: {} - {}", importer.getMinTime(), importer.getMaxTime());
        store = stBuilder.store();
        buildIndex();
    }

    private void buildIndex(){
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
    public void main(){
//        if(smallTest()) return;
        List<Long> rangeResult = queryByRange( 18300, 27000, 0, 200);


        List<IndexEntry> indexResult = queryByIndex(18300, 27000, 0, 200);
        Set<Long> indexResultSet = new HashSet<>();
        for(IndexEntry entry : indexResult){
            indexResultSet.add(entry.getEntityId());
            validateByRangeQuery(entry.getEntityId(), 1, entry.getStart(), entry.getEnd(), entry.getValue(0));
        }
        log.info("index result eid count {}", indexResultSet.size());

        Set<Long> set0 = new HashSet<>(rangeResult);
        log.info("intersection sets: {}", set0.retainAll(indexResultSet));
        Set<Long> commonEntities = set0;
        log.info("common eid count: {}", commonEntities.size());
        set0 = new HashSet<>(rangeResult);
        log.info("find diff in range result {}", set0.removeAll(commonEntities));
        log.info("diff in range result {}", set0.size());
        log.info("find diff in index result {}", indexResultSet.removeAll(commonEntities));
        log.info("diff in index result {}", indexResultSet.size());
        for(IndexEntry entry : indexResult){
            if(indexResultSet.contains(entry.getEntityId())) {
                validateByRangeQuery(entry.getEntityId(), 1, entry.getStart(), entry.getEnd(), entry.getValue(0));
            }
        }
    }

    private boolean smallTest() {
        store.getRangeValue(62254, 1, 0, 999999, new CustomCallBack(62254){
            public void onCall(int time, Slice value) { log.info("{} {}", time, value.getInt(0));}
            public Object onReturn() { log.info("onReturn of test"); return null;}
        });
        return true;
    }

    private List<IndexEntry> queryByIndex(int timeMin, int timeMax, int valueMin, int valueMax){
        IndexQueryRegion condition = new IndexQueryRegion(timeMin, timeMax);
        Slice minValue = new Slice(4);
        minValue.setInt(0, valueMin);
        Slice maxValue = new Slice(4);
        maxValue.setInt(0, valueMax);
        condition.add(new PropertyValueInterval(1, minValue, maxValue, IndexValueType.INT));
        List<IndexEntry> result = store.getEntries(condition);
        log.info("index result count {}", result.size());
        return result;
    }

    private List<Long> queryByRange(int timeMin, int timeMax, int valueMin, int valueMax){
        List<Long> result = new ArrayList<>();
        int i=0;
        for(Long entityId : importer.getRoadIdMap().values()){
            try {
                i++;
                if(i%1000==0)log.info("query {} entity", i);
                if(entityId==62254){
                    log.info("hit range query {}", entityId);
                }
                store.getRangeValue(entityId, 1, timeMin, timeMax,
                        new CustomCallBack(entityId){
                    private boolean first = true;
                    private int lastTime = -1;
                    private Slice lastVal;
                    public void onCall(int time, Slice value) {
                        if (first) {
                            first = false;
                        } else if (overlap(lastTime, time-1, timeMin, timeMax)) {
                            int val = lastVal.getInt(0);
                            if (valueMin<=val && val<=valueMax) throw new StopLoopException();
                        }
                        lastTime = time;
                        lastVal = value;
                    }
                    public Object onReturn() {
                        if(!first && lastTime<=timeMax){
                            int val = lastVal.getInt(0);
                            if (valueMin<=val && val<=valueMax) throw new StopLoopException();
                        }
                        return null;
                    }
                });
            }catch (StopLoopException e){
                result.add(entityId);
            }
        }
        log.info("iterate result count {}", result.size());
        return result;
    }

    private boolean validateByRangeQuery(long entityId, int proId, int timeMin, int timeMax, Slice value){
        try {
            if(entityId==62254){
                log.info("hit validate {}", entityId);
            }
            store.getRangeValue(entityId, proId, timeMin, timeMax, new CustomCallBack(entityId) {
                private boolean first = true;
                private int lastTime = -1;
                private Slice lastVal;
                public void onCall(int time, Slice value0) {
                    if (first) {
                        first = false;
                        if(time>timeMin) throw new StopLoopException("start time("+time+") later than timeMin");
                    } else if (overlap(lastTime, time-1, timeMin, timeMax)) {
                        if (!value.equals(value0)) throw new StopLoopException("value not equal, got "+value0.getInt(0));
                    }
                    lastTime = time;
                    lastVal = value0;
                }
                public Object onReturn() {
                    if(first){
                        throw new StopLoopException("no value in this time range ");
                    }else if(lastTime<=timeMax) {
                        if (!value.equals(lastVal)) {
                            throw new StopLoopException("value not equal, got " + lastVal.getInt(0));
                        }
                    }
                    return null;
                }
            });
        }catch (StopLoopException e){
            log.info("validate failed: {} {} {} {} {} {}",
                    entityId, proId, timeMin, timeMax, value.getInt(0), e.getMessage());
            return false;
        }
        return true;
    }

    private static boolean overlap(int t1min, int t1max, int t2min, int t2max){return (t1min<=t2max && t2min<=t1max);}

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
