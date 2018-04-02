package org.act.temporalProperty.impl.callback;

import org.act.temporalProperty.query.range.AbstractRangeQuery;
import org.act.temporalProperty.util.Slice;

public class CountCallBack extends AbstractRangeQuery
{

    int Number = 0;
    String valueType;
    
    @Override
    public void onNewValue(int time, Slice value)
    {
        Number++;
    }

    @Override
    public Object onReturn()
    {
        Slice toret = new Slice(4);
        toret.setInt( 0, Number );
        return toret;
    }

	@Override
	public void setValueType(String valueType) 
	{
		this.valueType = valueType;
	}

	@Override
	public CallBackType getType() {
		return CallBackType.COUNT;
	}

	@Override
	public void onCallBatch(Slice batchValue) {
		// TODO Auto-generated method stub
		
	}
}
