package org.act.temporalProperty.index;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.util.Slice;

import java.util.*;

/**
 * Created by song on 2018-01-21.
 */
public class IndexBuilderCallback {
    List<IndexPointEntry> data = new ArrayList<>();

    public void onCall(long entityId, int startTime, Slice value) {
        data.add(new IndexPointEntry(entityId,startTime, value));
    }

    public PeekingIterator<IndexIntervalEntry> getIterator(int endTime){
        data.sort((o1, o2) -> {
            if(o1.getEntityId()>o2.getEntityId()) {
                return 1;
            }else if(o1.getEntityId()<o2.getEntityId()){
                return -1;
            }else{
                return o1.getTimePoint() - o2.getTimePoint();
            }
        });
        return new IndexPoint2IntervalIterator(data.iterator(), endTime);
    }


}
