package edu.buaa.act.temporal.exception;

/**
 * Created by song on 17-12-6.
 */
public class TPSRuntimeException extends RuntimeException
{
    public TPSRuntimeException(String msg)
    {
        super(msg);
    }

    public TPSRuntimeException(Throwable e)
    {
        super(e);
    }
}
