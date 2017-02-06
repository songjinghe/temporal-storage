package org.act.dynproperty.impl.callback;

import org.act.dynproperty.impl.RangeQueryCallBack;
import org.act.dynproperty.util.Slice;

public class CountCallBack implements RangeQueryCallBack
{

    int Number = 0;
    String valueType;
    
    @Override
    public void onCall( Slice value )
    {
        Number++;
    }

    @Override
    public Slice onReturn()
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
