package edu.buaa.act.temporal.exception;

/**
 * Created by song on 2018-1-5.
 *
 * Exceptions which should not happen.
 * occurs only when db code contains bug.
 */
public class TPSNHException extends TPSRuntimeException
{
    public TPSNHException(String msg)
    {
        super(msg);
    }

    public TPSNHException(Throwable e)
    {
        super(e);
    }
}
