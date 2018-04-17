package org.act.temporalProperty.helper;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 2018-04-16.
 */
public class StoreLock{
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Condition memTableSubmitted = lock.writeLock().newCondition();
    private final Condition mergeDone = lock.writeLock().newCondition();
    private boolean systemDown = false;

    public void shutdown(){
        this.systemDown = true;
    }

    public void lockExclusive() throws InterruptedException {
        lock.writeLock().lock();
        if(systemDown){
            throw new InterruptedException("System already shutdown!");
        }// else return;
    }

    public void unlockExclusive() throws InterruptedException {
        lock.writeLock().unlock();
    }

    public void mergeLockExclusive() throws InterruptedException {
        lock.writeLock().lock();
    }

    public void mergeUnlockExclusive() throws InterruptedException {
        lock.writeLock().unlock();
    }

    public void lockShared() throws InterruptedException {
        lock.readLock().lock();
        if(systemDown){
            throw new InterruptedException("System already shutdown!");
        }
    }

    public void unlockShared() {
        lock.readLock().unlock();
    }

    public void waitSubmitMemTable() throws InterruptedException {
        memTableSubmitted.await();
        if(systemDown){
            throw new InterruptedException("System already shutdown!");
        }
    }

    public void memTableSubmitted(){
        memTableSubmitted.signal();
    }

    public void waitMergeDone() throws InterruptedException {
        mergeDone.await();
    }

    public void mergeDone(){
        mergeDone.signal();
    }

}
