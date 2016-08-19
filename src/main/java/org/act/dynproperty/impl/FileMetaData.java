package org.act.dynproperty.impl;

import com.google.common.base.Function;

import java.util.concurrent.atomic.AtomicInteger;

import org.act.dynproperty.util.Slice;

/**
 * 存储文件元信息，包括文件名，负责存储的有效时间段等
 *
 */
public class FileMetaData
{
    public static final Function<FileMetaData, Integer> GET_LARGEST_USER_KEY = new Function<FileMetaData, Integer>()
    {
        @Override
        public Integer apply(FileMetaData fileMetaData)
        {
            return fileMetaData.getLargest();
        }
    };

    /**
     * 文件编码，起文件名作用。
     */
    private final long number;

    /**
     * 文件大小，以byte为单位
     */
    private final long fileSize;

    /**
     * 负责存储有效时间的起始时间
     */
    private final int smallest;

    /**
     * 负责存储有效时间的结束席间
     */
    private final int largest;
    // todo this mutable state should be moved elsewhere
    private final AtomicInteger allowedSeeks = new AtomicInteger(1 << 30);

    /**
     * 实例化
     * @param number 文件编号
     * @param fileSize 文件大小
     * @param smallest 有效时间的起始时间
     * @param largest 有效时间的结束时间
     */
    public FileMetaData(long number, long fileSize, int smallest, int largest)
    {
        this.number = number;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public long getNumber()
    {
        return number;
    }

    public int getSmallest()
    {
        return smallest;
    }

    public int getLargest()
    {
        return largest;
    }

    public int getAllowedSeeks()
    {
        return allowedSeeks.get();
    }

    public void setAllowedSeeks(int allowedSeeks)
    {
        this.allowedSeeks.set(allowedSeeks);
    }

    public void decrementAllowedSeeks()
    {
        allowedSeeks.getAndDecrement();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("FileMetaData");
        sb.append("{number=").append(number);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        sb.append(", allowedSeeks=").append(allowedSeeks);
        sb.append('}');
        return sb.toString();
    }
}
