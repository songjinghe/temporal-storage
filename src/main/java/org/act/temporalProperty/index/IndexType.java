package org.act.temporalProperty.index;

import com.google.common.base.Preconditions;

/**
 * Created by song on 2018-01-18.
 */
public enum IndexType {
    TIME_AGGR(0), SINGLE_VALUE(1), MULTI_VALUE(2);

    int id;
    IndexType(int id){
        this.id = id;
    }

    public int getId(){return id;}

    public static IndexType decode(int i){
        Preconditions.checkArgument(i==0 || i==1 || i==2);
        switch (i){
            case 0: return TIME_AGGR;
            case 1: return SINGLE_VALUE;
            default:return MULTI_VALUE;
        }
    }
}
