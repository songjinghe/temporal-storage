package org.act.temporalProperty.impl.query.range;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.query.range.AbstractRangeQuery;
import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.StoreBuilder;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by song on 2018-03-27.
 */
public class CorrectnessTest {
    private static Logger log = LoggerFactory.getLogger(CorrectnessTest.class);
    private String dbDir(){
        if(SystemUtils.IS_OS_WINDOWS){
            return "temporal.property.test";
        }else{
            return "/tmp/temporal.property.test";
        }
    }

    @Test // to fail
    public void unstableNotQuery() throws Throwable {
        StoreBuilder stBuilder = new StoreBuilder(dbDir(), true);
        TemporalPropertyStore store = stBuilder.store();
        store.createProperty(0, ValueContentType.INT);
        int sep=999999;
        for(long i=0; i<sep; i++) {
            StoreBuilder.setIntProperty(store, (int)i, 1, 0, (int)i);
        }
        for(long i=sep; i<2*sep; i++) {
            StoreBuilder.setIntProperty(store, (int)i, 2, 0, (int)i);
        }
        Slice val = store.getPointValue(1,0, sep*2-10);
        log.debug("point query result {}", val.getInt(0));
        log.debug("if you see next line, then means no bug.");
        Object result = store.getRangeValue(1, 0, sep*2-10, sep*2, new AbstractRangeQuery(){

            @Override
            public void setValueType(String valueType) {
                //
            }

            @Override
            public void onNewValue(int time, Slice value) {
                log.debug("if you see this line, then means no bug. val={}", value.getInt(0));
            }

            @Override
            public void onCallBatch(Slice batchValue) {}

            @Override
            public Object onReturn() {
                return null;
            }

            @Override
            public CallBackType getType() {
                return null;
            }
        });

        store.shutDown();
    }

}
