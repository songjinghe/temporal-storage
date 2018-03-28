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

    private final LoadingCache<String, IndexTableFile> cache;

    public IndexTableCache(final File indexDir, int tableCacheSize)
    {
        Preconditions.checkNotNull(indexDir, "databaseName is null");
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .build(new CacheLoader<String, IndexTableFile>(){
                    @Override
                    public IndexTableFile load(String fileAbsPath) throws IOException{
                        return new IndexTableFile(fileAbsPath);
                    }
                });
    }

    public IndexTable getTable(String absPath)
    {
        IndexTable table;
        try {
            table = cache.get(absPath).getTable();
        } catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + absPath, cause);
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
     * @param fileAbsPath 文件编号
     */
    public void evict(String fileAbsPath){
        cache.invalidate(fileAbsPath);
    }

    private static final class IndexTableFile
    {
        private final IndexTable table;
        private final FileChannel fileChannel;

        private IndexTableFile(String fileAbsPath) throws IOException{
            fileChannel = new RandomAccessFile(new File(fileAbsPath),"rw").getChannel();
            table = new IndexTable(fileChannel);
        }

        public IndexTable getTable()
        {
            return table;
        }
    }
}
