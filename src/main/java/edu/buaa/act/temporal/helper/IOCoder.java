package edu.buaa.act.temporal.helper;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by song on 17-12-26.
 */
public interface IOCoder<T>
{
    int rawSize();
    T decode(byte[] src);
    T decode(DataInput in) throws IOException;
    T decode(ByteBuffer in);
    byte[] encode(T t);



}
