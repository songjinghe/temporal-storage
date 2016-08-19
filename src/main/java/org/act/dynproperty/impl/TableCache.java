
package org.act.dynproperty.impl;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import org.act.dynproperty.table.FileChannelTable;
import org.act.dynproperty.table.MMapTable;
import org.act.dynproperty.table.Table;
import org.act.dynproperty.table.UserComparator;
import org.act.dynproperty.util.Closeables;
import org.act.dynproperty.util.Finalizer;
import org.act.dynproperty.util.InternalTableIterator;
import org.act.dynproperty.util.Slice;

/**
 * 对所有存储文件的缓存机制，使用FIFO的方式进行，如果需要对某个文件进行访问，可以直接从TableCache这里对文件的Iterator进行查询
 *
 */
public class TableCache
{
    private final LoadingCache<Long, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);;

    public TableCache(final File databaseDir, int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums, final boolean isStableFile)
    {
        Preconditions.checkNotNull(databaseDir, "databaseName is null");
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
//                .removalListener(new RemovalListener<Long, TableAndFile>()
//                {
//                    @Override
//                    public void onRemoval(RemovalNotification<Long, TableAndFile> notification)
//                    {
//                        Table table = notification.getValue().getTable();
//                        finalizer.addCleanup(table, table.closer());
//                    }
//                })
                .build(new CacheLoader<Long, TableAndFile>()
                {
                    @Override
                    public TableAndFile load(Long fileNumber)
                            throws IOException
                    {
                        return new TableAndFile(databaseDir, fileNumber, userComparator, verifyChecksums, isStableFile);
                    }
                });
    }

    /**
     * 通过需要查询文件的元信息，得到相应文件的Iterator
     * @param file 文件元信息
     * @return 相应文件的Iterator
     */
    public SeekingIterator<Slice,Slice> newIterator(FileMetaData file)
    {
        return newIterator(file.getNumber());
    }

    /**
     * 通过文件的编号，得到相应文件的Iterator
     * @param number 文件编号
     * @return 相应文件的Iterator
     */
    public SeekingIterator<Slice,Slice> newIterator(long number)
    {
        return getTable(number).iterator();
    }
    
    
    public Table newTable( long number )
    {
        return getTable( number );
    }

    public long getApproximateOffsetOf(FileMetaData file, Slice key)
    {
        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }

    private Table getTable(long number)
    {
        Table table;
        try {
            table = cache.get(number).getTable();
        }
        catch (ExecutionException e) {
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
    public void close()
    {
        cache.invalidateAll();
        finalizer.destroy();
    }

    /**
     * 将某个文件从缓存中排除
     * @param number 文件编号
     */
    public void evict(long number)
    {
        cache.invalidate(number);
    }

    private static final class TableAndFile
    {
        private final Table table;
        private final FileChannel fileChannel;

        private TableAndFile(File databaseDir, long fileNumber, UserComparator userComparator, boolean verifyChecksums, boolean isStableFile)
                throws IOException
        {
            String tableFileName;
            if( isStableFile )
                tableFileName = Filename.stableFileName(fileNumber);
            else
                tableFileName = Filename.unStableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);
            fileChannel = new RandomAccessFile(tableFile,"rw").getChannel();
            try {
                //FIXME 
                if ( true ) {
                    table = new MMapTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyChecksums);
                }
                else {
                    table = new FileChannelTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyChecksums);
                }
            }
            catch (IOException e) {
                Closeables.closeQuietly(fileChannel);
                throw e;
            }
        }

        public Table getTable()
        {
            return table;
        }
    }
}
