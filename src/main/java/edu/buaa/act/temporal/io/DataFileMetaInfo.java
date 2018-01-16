package edu.buaa.act.temporal.io;

import com.google.common.base.Objects;
import edu.buaa.act.temporal.TimePoint;
import edu.buaa.act.temporal.ValueAtTime;

/**
 * Created by song on 2018-01-02.
 */
public class DataFileMetaInfo implements ValueAtTime
{
    public enum TYPE{
        MEM_LOG, UN_STABLE, STABLE, STABLE_BUFFER, INDEX_STABLE, INDEX_UN_STABLE
    }

    private final TYPE type;
    private final int propertyId;
    private final long id;

    private boolean bufferFile; // for stable file only
    private long fileSize;
    private TimePoint smallest;
    private TimePoint largest;

    public DataFileMetaInfo( long id, int propertyId, TYPE type )
    {
        this.type = type;
        this.propertyId = propertyId;
        this.id = id;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public long getId()
    {
        return id;
    }

    public TYPE getType()
    {
        return type;
    }

    public int getPropertyId()
    {
        return propertyId;
    }

    public boolean hasBufferFile()
    {
        return bufferFile;
    }

    @Override
    public boolean comparable()
    {
        return false;
    }

    @Override
    public boolean isInvalid()
    {
        return false;
    }

    @Override
    public boolean isUnknown()
    {
        return false;
    }

    @Override
    public byte[] encode()
    {
        //return new byte[0];
    }

    @Override
    public int length()
    {
        //return 0;
    }

    @Override
    public int compareTo(ValueAtTime o)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("FileMetaData");
        sb.append("{number=").append(id);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFileMetaInfo that = (DataFileMetaInfo) o;
        return propertyId == that.propertyId &&
                id == that.id &&
                type == that.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, propertyId, id);
    }
}


//    public static final Function<DataFileMetaInfo, Integer> GET_LARGEST_USER_KEY = DataFileMetaInfo::getLargest;