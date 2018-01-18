package org.act.temporalProperty.impl;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.util.Slice;

import static org.act.temporalProperty.impl.TemporalPropertyStoreImpl.toSlice;

/**
 * Created by song on 2018-01-17.
 */
public class SingleTPStore {
    private PropertyMetaData meta;
    private UnstableLevel unLevel;
    private StableLevel stlevel;

    /**
     * 进行时间点查询，参考{@link TemporalPropertyStore}中的说明
     */
    public Slice getPointValue(long id, int time )
    {
        Slice idSlice = toSlice(meta.getPropertyId(), id);
        if( time >= this.stlevel.getTimeBoundary() )
        {
            Slice result = this.unLevel.getPointValue( idSlice, time );
            if( null == result )
                return this.stlevel.getPointValue( idSlice, time );
            else if( result.length() == 0 )
                return null;
            else
                return result;
        }
        else
            return this.stlevel.getPointValue( idSlice, time );
    }

    /**
     * 进行实践段查询，参考{@link TemporalPropertyStore}中的说明
     */
    public Object getRangeValue( long id, int startTime, int endTime, RangeQueryCallBack callback )
    {
        Slice idSlice = toSlice(meta.getPropertyId(), id);
        if( startTime < this.stlevel.getTimeBoundary() )
            this.stlevel.getRangeValue( idSlice, startTime, Math.min( (int)this.stlevel.getTimeBoundary(), endTime ), callback );
        if( endTime >= this.stlevel.getTimeBoundary() )
            this.unLevel.getRangeValue( idSlice, Math.max( (int)this.stlevel.getTimeBoundary(), startTime ), endTime, callback );
        return callback.onReturn();
    }

}
