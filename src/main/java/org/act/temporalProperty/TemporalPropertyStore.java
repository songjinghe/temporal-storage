package org.act.temporalProperty;

import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.index.IndexQueryRegion;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.util.Slice;

import java.util.List;

/**
 * 动态属性存储系统，对外提供其功能的接口
 *
 */
public interface TemporalPropertyStore
{
	/**
	 * Get this by executing:
	 * echo https://github.com/TGraphDB/ | sha1sum
	 */
	String MagicNumber = "c003bf3c9563aa283d49c17fc13f736e5493107c"; //40bytes==160bits

	int Version = 1;
	/**
	 * 对某个动态属性进行时间点查询，返回查询的 结果
	 * @param entityId 动态属性所属的点/边的id
	 * @param proId 动态属性id
	 * @param time 需要查询的时间
	 * @return @{Slice} 查询的结果
	 */
    Slice getPointValue( long entityId, int proId, int time );
    
    /**
	 * 对某个动态属性进行时间段查询，返回查询的 结果
	 * @param id 动态属性所属的点/边的id
	 * @param proId 动态属性id
	 * @param startTime 需要查询的时间的起始时间
	 * @param endTime 需要查询的时间的结束时间
	 * @param callback 时间段查询所采用的聚集类型
	 * @return @{Slice} 查询的结果
	 */
    Object getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback );

	/**
	 * 创建某个动态属性
	 * @param propertyId 动态属性的id
	 * @return 是否创建成功，如果有相同ID但类型不同的属性则返回false
	 */
	boolean createProperty(int propertyId, ValueContentType type);

    /**
     * 写入某个动态属性的值
     * @param id 动态属性所属的点/边的id+动态属性id+相应值有效的起始时间
     * @param value 值
     * @return 是否写入成功
     */
    boolean setProperty( Slice id, byte[] value );
    
    /**
     * 删除某个动态属性
     * @param propertyId 动态属性的id
     * @return 是否删除成功
     */
    boolean deleteProperty(int propertyId);

	/**
	 * 删除某个动态属性中某个eid的所有数据
	 * @param id 动态属性的id + entity id
	 * @return 是否删除成功
	 */
	boolean deleteEntityProperty(Slice id);

	/**
	 * 创建一个值索引
	 * @param start  索引开始时间
	 * @param end    索引结束时间
	 * @param proIds 索引的属性id列表
	 */
	void createValueIndex(int start, int end, List<Integer> proIds);

	/**
	 * get entity id which satisfy query condition
	 * @param condition query condition of one property
	 * @return null if no index available;
	 */
	List<Long> getEntities(IndexQueryRegion condition);

	/**
	 * get index entries which satisfy query condition
	 * @param condition query condition of one property
	 * @return null if no index available;
	 */
	List<IndexEntry> getEntries(IndexQueryRegion condition);

	void flushMemTable2Disk();

    void flushMetaInfo2Disk();

    void shutDown();
}
