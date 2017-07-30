package org.act.temporalProperty;

import org.act.temporalProperty.impl.FileMetaData;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.util.Slice;


/**
 * Level接口，UnStableFile在level0中维护，StableFile在level1中维护
 *
 */
public interface Level
{
	/**
	 * 进行时间点查询
	 * @param idSlice 用于唯一标识动态属性的id
	 * @param time 时间点
	 * @return 返回值
	 */
    public Slice getPointValue( Slice idSlice, int time );
    
    /**
     * 进行时间段查询
     * @param idSlice 用于唯一标识动态属性的id
     * @param startTime 时间段起始时间
     * @param endTime 时间段结束时间
     * @param callback 时间段查询对值的聚集类型
     */
    public void getRangeValue( Slice idSlice, int startTime, int endTime, RangeQueryCallBack callback );
    
    /**
     * 向level中写入动态属性数据
     * @param key
     * @param value
     * @return
     */
    public boolean set( InternalKey key, Slice value );
    
    /**
     * 在系统启动阶段，初始化属于自己level的存储文件
     * @param metaData
     */
    public void initfromdisc( FileMetaData metaData );
}
