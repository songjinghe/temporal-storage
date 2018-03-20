package org.act.temporalProperty.index;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.act.temporalProperty.impl.Filename;
import org.act.temporalProperty.util.Closeables;
import org.act.temporalProperty.util.Slice;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * Created by song on 2018-01-19.
 */
public class IndexTableCache {

    private final LoadingCache<Long, IndexTableFile> cache;

    public IndexTableCache(final File indexDir, int tableCacheSize)
    {
        Preconditions.checkNotNull(indexDir, "databaseName is null");
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .build(new CacheLoader<Long, IndexTableFile>(){
                    @Override
                    public IndexTableFile load(Long fileNumber) throws IOException{
                        return new IndexTableFile(indexDir, fileNumber);
                    }
                });
    }

//    public Iterator<Slice> newIterator(IndexFileMeta file)
//    {
//        return newIterator(file.getNo());
//    }
//
//    /**
//     * 通过文件的编号，得到相应文件的Iterator
//     * @param number 文件编号
//     * @return 相应文件的Iterator
//     */
//    public Iterator<Slice> newIterator(long number)
//    {
//        return getTable(number).iterator();
//    }

    private IndexTable getTable(long number)
    {
        IndexTable table;
        try {
            table = cache.get(number).getTable();
        } catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    /**
     * 关闭缓存，将缓存在内存中的文件channel关闭
     */
    public void close(){
        cache.invalidateAll();
    }

    /**
     * 将某个文件从缓存中排除
     * @param number 文件编号
     */
    public void evict(long number){
        cache.invalidate(number);
    }

    private static final class IndexTableFile
    {
        private final IndexTable table;
        private final FileChannel fileChannel;

        private IndexTableFile(File indexDir, long fileNumber) throws IOException{
            String tableFileName = Filename.unStableFileName(fileNumber);
            File tableFile = new File(indexDir, tableFileName);
            fileChannel = new RandomAccessFile(tableFile,"rw").getChannel();
            table = new IndexTable(fileChannel);
        }

        public IndexTable getTable()
        {
            return table;
        }
    }
}
