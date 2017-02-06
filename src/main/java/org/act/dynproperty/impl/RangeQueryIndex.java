package org.act.dynproperty.impl;

import org.act.dynproperty.util.Slice;
import static org.act.dynproperty.impl.callback.CountCallBack.*;

import java.util.Map.Entry;

public class RangeQueryIndex {
	
	private String dbDir;
	private IndexTableCache tableCache;
	
	
	public Slice get(int fileNumber, CallBackType type, Slice idSlice ){
		SeekingIterator<Slice, Slice> iterator = this.tableCache.newIterator( fileNumber );
		InternalKey searchKey = null;
		switch (type) {
		case COUNT:
			searchKey = new InternalKey(idSlice, 0, 0, ValueType.VALUE);
			break;
		case MAX:
			searchKey = new InternalKey(idSlice, 1, 0, ValueType.VALUE);
			break;
		case MIN:
			searchKey = new InternalKey(idSlice, 2, 0, ValueType.VALUE);
			break;
		case SUM:
			searchKey = new InternalKey(idSlice, 3, 0, ValueType.VALUE);
			break;
		default:
			throw new RuntimeException("User defined callback should not use index");
			
		}
		iterator.seek(searchKey.encode());
		if(iterator.hasNext()){
			Entry<Slice, Slice> entry = iterator.next();
			return entry.getKey();
		} else {
			return null;
		}
	}
	

}
