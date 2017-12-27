package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.impl.table.MemTableEntry;
import edu.buaa.act.temporal.impl.table.MemTableUnsorted;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by song on 17-12-19.
 *
 * method without synchronized because they can be only called from one thread at a time.
 */
public class ManageProcess extends Thread
{
    private BlockingQueue<MemTableUnsorted> queue = new LinkedBlockingQueue<>();
    private volatile boolean shouldRun;


    public void shutdown()
    {
        this.shouldRun = true;
    }

    @Override
    public void run()
    {
        while(shouldRun)
        {
            try
            {
                MemTableUnsorted memTable = queue.take();

                mergeToDisk(memTable);

            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }


        }
    }

    private void mergeToDisk(MemTableUnsorted memTable)
    {
        List<MemTableEntry> data = memTable.getData();
        for(MemTableEntry entry : data){

        }
    }


}
