package org.act.temporalProperty.index;

import com.google.common.base.Preconditions;

/**
 * Created by song on 2018-01-18.
 */
public enum IndexType {
    TIME_AGGR(0), VALUE(1);

    int id;
    IndexType(int id){
        this.id = id;
    }

    public int getId(){return id;}

    public static IndexType decode(int i){
        Preconditions.checkArgument(i==0 || i==1);
        if(i==0) return TIME_AGGR;
        else return VALUE;
    }
}
