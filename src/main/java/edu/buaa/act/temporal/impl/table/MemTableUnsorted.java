package edu.buaa.act.temporal.impl.table;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.*;
import com.google.common.primitives.Ints;
import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.impl.TemporalValue;
import org.act.temporalProperty.impl.FileChannelLogWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by song on 17-12-6.
 */
public class MemTableUnsorted
{
    private List<MemTableEntry> data = new LinkedList<>();
    private Set<Integer> deletedTPSet = new HashSet<Integer>();

    public ValueAtTime getValueAtTimePoint(int propertyId, long entityId, TimePoint time)
    {
        for (MemTableEntry tve : data)
        {
            if(tve.getPropertyId()==propertyId && tve.getEntityId()==entityId && tve.getTime().contains(time) )
            {
                return tve.getValue();
            }
        }
        return null;
    }

    public List<TimeValueEntry> getValuesDuringTimeInterval(int propertyId, long entityId, TimeInterval timeRange)
    {
        TemporalValue tv = new TemporalValue(propertyId, entityId);

        for (MemTableEntry tve : data)
        {
            if(tve.getPropertyId()==propertyId && tve.getEntityId()==entityId && tve.getTime().overlap(timeRange) )
            {
                tv.put(timeRange, tve.getValue());
            }
        }
        return Collections.emptyList();
    }

    public void setValueDuringTimeInterval(int propertyId, long entityId, TimePoint start, TimePoint end, ValueAtTime value)
    {
        data.add(new MemTableEntry(propertyId, entityId, new TimeInterval(start, end), value));
    }



    public void deleteTemporalProperty(int propertyId)
    {
        this.deletedTPSet.add(propertyId);
    }

    public List<MemTableEntry> getData()
    {
        return data;
    }

    public void load(File file) throws IOException
    {
        FileChannel channel = new FileInputStream(file).getChannel();
        MappedByteBuffer in = channel.map(FileChannel.MapMode.READ_ONLY, 0, 4096);
        int versionLen = in.getInt();
        byte[] strContent = new byte[versionLen];
        in.get(strContent);
        long time = in.getLong();
        byte[] hash = new byte[16];
        in.get(hash);
        int contentLen = in.getInt();
        for(int l=0; l<contentLen; l++)
        {
            int pid = in.getInt();
            long eid = in.getLong();
            TimePoint start = TimePoint.IO.decode(in);
            TimePoint end = TimePoint.IO.decode(in);
            int vLen = in.getInt();
            ValueAtTime v = ValueAtTime.decode(in, vLen);
            MemTableEntry e = new MemTableEntry(pid, eid, new TimeInterval(start, end), v);
            data.add(e);
            l+=(4+8+TimePoint.IO.rawSize()*2+4+vLen);
        }
        channel.close();
    }

    public void dump(File file) throws IOException
    {
        ByteArrayDataOutput content = ByteStreams.newDataOutput(data.size()*24);
        for (MemTableEntry e : data)
        {
            content.writeInt(e.getPropertyId());
            content.writeLong(e.getEntityId());
            content.write(TimePoint.IO.encode(e.getTime().getStart()));
            content.write(TimePoint.IO.encode(e.getTime().getEnd()));
            content.writeInt(e.getValue().length());
            content.write(e.getValue().encode());
        }
        byte[] contentBytes = content.toByteArray();
        HashCode hash = Hashing.md5().hashBytes(contentBytes);

        FileChannel channel = new FileOutputStream(file).getChannel();
        MappedByteBuffer out = channel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);

        out.putInt(TemporalPropertyStorage.version.length());
        out.put(TemporalPropertyStorage.version.getBytes());
        out.putLong(System.currentTimeMillis());
        out.put(hash.asBytes());
        out.putInt(contentBytes.length);
        out.put(contentBytes);

        out.force();
        channel.force(true);
        channel.close();
    }
}
