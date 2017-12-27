package edu.buaa.act.temporal.helper;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 17-12-27.
 */
public abstract class LockingLayer implements Runnable
{
    private ReadWriteLock sysLock = new ReentrantReadWriteLock();

    public void go()
    {

    }
}
