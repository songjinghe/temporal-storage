package org.act.temporalProperty.exception;

/**
 * Created by song on 2018-01-17.
 */
public class TPSRuntimeException extends RuntimeException {
    public TPSRuntimeException(String msg, Throwable e){
        super(msg, e);
    }
    public TPSRuntimeException(String msg){
        super(msg);
    }
}
