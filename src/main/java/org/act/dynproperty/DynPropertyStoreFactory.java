package org.act.dynproperty;

import org.act.dynproperty.impl.DynPropertyStoreImpl;
/**
 * 新建动态属性存储的工厂类
 *
 */
public class DynPropertyStoreFactory
{
	/**
	 * 返回新的动态属性存储实例
	 * @param dbDir 保存动态属性存储文件的目录
	 * @return
	 */
    public static DynPropertyStore newPropertyStore( String dbDir ) throws Throwable {
        return new DynPropertyStoreImpl( dbDir );
    }
}
