package org.act.temporalProperty.index;

import com.google.common.collect.Multimap;
import org.act.temporalProperty.impl.TableCache;

import java.util.*;

/**
 * Created by song on 2018-01-18.
 */
public class TemporalValueIndex {
    private int nextIndexId=0;
    private Multimap<Integer, IndexMetaData> indexes;
    private TableCache cache;

    public TemporalValueIndex(TableCache cache) {
        this.cache = cache;
    }


    public boolean contains(IndexQueryRegion conditions) {
        return !getCommonIndex(conditions).isEmpty();
    }

    public List<Long> query(IndexQueryRegion condition) {
        Collection<IndexMetaData> index = getCommonIndex(condition);
        if(!index.isEmpty()){

        }else {

        }
        return null;
    }

    public List<Long> query(List<IndexQueryRegion> conditions) {
        return null;
    }

    private List<IndexMetaData> getCommonIndex(IndexQueryRegion conditions){
        Set<IndexMetaData> meta=null, pre=null;
        for(int i=0; i<conditions.getPropertyValueIntervals().size(); i++){
            PropertyValueInterval c = conditions.getPropertyValueIntervals().get(i);
            meta = new HashSet<>(indexes.get(c.getProId()));
            if(i>0){
                meta.retainAll(pre);
            }
            pre = meta;
        }
        if(meta!=null && !meta.isEmpty()){
            return new ArrayList<>(meta);
        }else{
            return Collections.emptyList();
        }
    }
}
