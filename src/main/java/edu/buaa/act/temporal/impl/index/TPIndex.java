package edu.buaa.act.temporal.impl.index;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import java.io.DataOutput;

/**
 * Created by song on 17-12-20.
 */
public class TPIndex
{

    private Integer id;

    public void encode(DataOutput out)
    {

    }

    public static TPIndex decode(ByteArrayDataInput in)
    {
        return new TPIndex();
    }

    public Integer getId()
    {
        return id;
    }
}
