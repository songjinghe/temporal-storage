package me;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 17-12-30.
 */
public class ConditionLockTest
{
    static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


    public static void main(String[] args) throws InterruptedException
    {

        Condition cond = lock.writeLock().newCondition();

        new Thread(() ->
        {
            try
            {
                cond.await();
                System.out.println("hehe");
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }).start();


        System.out.println("haha");
        Thread.sleep(1000);
        cond.signalAll();
    }
}
