package org.act.temporalProperty;

import org.act.temporalProperty.impl.TemporalPropertyStoreImpl;
/**
 * 新建动态属性存储的工厂类
 *
 */
public class TemporalPropertyStoreFactory
{
	/**
	 * 返回新的动态属性存储实例
	 * @param dbDir 保存动态属性存储文件的目录
	 * @return
	 */
    public static TemporalPropertyStore newPropertyStore(String dbDir ) throws Throwable {
        return new TemporalPropertyStoreImpl( dbDir );
    }
}
