package org.act.temporalProperty.exception;

/**
 * Created by song on 2018-01-17.
 */
public class TPSException extends Exception {

    public TPSException(String s) {
        super(s);
    }

    public TPSException(String msg, Throwable e){
        super(msg, e);
    }
}
