package org.act.graph.io;

public class IndexStore
{
    private final int BLOCK_RECORD_NUM = 50;
    private final int BLOCK_SIZE = 1 + 8 + IndexRecord.RECORD_SIZE * BLOCK_RECORD_NUM;
    
    private String dir;
    
    private IdFactory idfactory;
}
