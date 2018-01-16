package edu.buaa.act.temporal.impl.table;

import com.google.common.collect.PeekingIterator;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.exception.TPSException;
import edu.buaa.act.temporal.exception.TPSRuntimeException;
import edu.buaa.act.temporal.impl.TimePointValueEntry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created by song on 17-12-11.
 */
public class MemTableSorted
{
    private static final String fileHeader = TemporalPropertyStorage.version+" MEMTABLE";
    private TreeMap<PETKey, ValueAtTime> data = new TreeMap<>();
    private Map<Integer, Integer> propertySize = new HashMap<>();
    private long currentSize=0;

    public ValueAtTime getValueAtTimePoint(int propertyId, long entityId, TimePoint time)
    {
        PETKey searchKey = new PETKey(propertyId, entityId, time);
        Entry<PETKey, ValueAtTime> s = data.floorEntry(searchKey);
        if(s!=null){
            PETKey got = s.getKey();
            if(got.getPropertyId()==propertyId && got.getEntityId()==entityId)
            {
                if(s.getValue().equals(ValueAtTime.Unknown))
                {
                    if (!s.getKey().getTime().equals(TimePoint.INIT))
                    {
                        return getValueAtTimePoint(propertyId, entityId, s.getKey().getTime().pre());
                    } else
                    {
                        return s.getValue();
                    }
                }else{
                    return s.getValue();
                }
            }else{
                return ValueAtTime.Unknown;
            }
        }else{
            return ValueAtTime.Unknown;
        }
    }

    public PeekingIterator<TimePointValueEntry> getTimePointValues(int propertyId, long entityId, TimePoint start)
    {
        PETKey s = new PETKey(propertyId, entityId, start);

        NavigableMap<PETKey, ValueAtTime> subMap = data.tailMap(s, true);

        Entry<PETKey, ValueAtTime> pre = data.floorEntry(s);

        if(pre!=null && !pre.getKey().equals(s)) // pre key not null and not equal to searchKey
        {
            PETKey got = pre.getKey();
            if(got.getPropertyId()==propertyId && got.getEntityId()==entityId)
            {
                return new MemTimePointValueIterator(subMap, propertyId, entityId, pre.getKey(), pre.getValue());
            }
        }
        return new MemTimePointValueIterator(subMap, propertyId, entityId);
    }

    public void set(int propertyId, long entityId, TimePoint start, TimePoint end, ValueAtTime value)
    {
        if(value.isUnknown()){
            throw new TPSRuntimeException("value can not be Unknown");
        }

        PETKey s = new PETKey(propertyId, entityId, start);
        PETKey e = new PETKey(propertyId, entityId, end);

        Entry<PETKey, ValueAtTime> ss = data.floorEntry(s);
        Entry<PETKey, ValueAtTime> ee = data.floorEntry(e);

        if(ss!=null &&
                ss.getKey().getPropertyId()==propertyId &&
                ss.getKey().getEntityId()==entityId &&
                ss.getValue().equals(value)){
            // equal value, no need to remove or add. leave it as it is. merged.
        }else{
            data.put(s, value); // update current or insert a new entry
            incPropertySize(propertyId, value.length());
        }

        if(end.isNow()){
            // do nothing.
        }else
        {
            if (ee != null && ee.getKey().getEntityId() == entityId && ee.getKey().getPropertyId() == propertyId)
            {
                PETKey ePost = new PETKey(propertyId, entityId, end.post());
                data.put(ePost, ee.getValue());
                incPropertySize(propertyId, ee.getValue().length());
            } else
            {
                PETKey ePost = new PETKey(propertyId, entityId, end.post());
                data.put(ePost, ValueAtTime.Unknown);
                Entry<PETKey, ValueAtTime> higher = data.higherEntry(ePost);
                if (higher != null &&
                        higher.getKey().getPropertyId() == propertyId &&
                        higher.getKey().getEntityId() == entityId &&
                        higher.getValue().isUnknown())
                {
                    data.remove(higher.getKey());
                }
            }
        }

        NavigableMap<PETKey, ValueAtTime> sub = data.subMap(s, false, e, true);

        for(Entry<PETKey, ValueAtTime> entry : sub.entrySet()){
            data.remove(entry.getKey());
            incPropertySize(propertyId, 0-entry.getValue().length());
        }
    }

    public void deleteTemporalProperty(int propertyId)
    {
        List<PETKey> tpEntries = new ArrayList<>();
        for(Entry<PETKey, ValueAtTime> entry : data.entrySet())
        {
            if(entry.getKey().getPropertyId()==propertyId)
            {
                tpEntries.add(entry.getKey());
            }
        }
        for(PETKey key : tpEntries)
        {
            data.remove(key);
        }
    }

    private void incPropertySize(int propertyId, int delta)
    {
        propertySize.merge(propertyId, delta, (a, b) -> a + b);
        currentSize += delta;
    }

    public long getCurrentSize(){
        return currentSize;
    }

    public long getCurrentCount(){
        return data.size();
    }

    public void load(File file) throws IOException, TPSException
    {
        ByteBuffer in = validate(file);

        while(in.hasRemaining())
        {
            int pid = in.getInt();
            long eid = in.getLong();
            TimePoint time = TimePoint.IO.decode(in);
            int vLen = in.getInt();
            ValueAtTime value = ValueAtTime.decode(in);
            PETKey key = new PETKey(pid, eid, time);
            data.put(key, value);
            currentSize+=value.length();
        }
    }

    public ByteBuffer validate(File file) throws IOException, TPSException
    {
        return validate(Files.toByteArray(file));
    }

    public ByteBuffer validate(byte[] raw) throws TPSException
    {
        ByteBuffer in = ByteBuffer.wrap(raw);

        int versionLen = in.getInt();
        byte[] strContent = new byte[versionLen];
        in.get(strContent);
        if(!fileHeader.equals(new String(strContent))){
            throw new TPSException("invalid memtable file: header not match");
        }
        long time = in.getLong();
        if(time>System.currentTimeMillis()){
            throw new TPSException("invalid memtable file: time later than now");
        }
        byte[] hash = new byte[16];
        in.get(hash);
        int contentLen = in.getInt();
        if(in.remaining()!=contentLen){
            throw new TPSException("invalid memtable file: content length not match");
        }

        HashCode hashCode = Hashing.md5().hashBytes(raw, in.position(), contentLen);
        if(Bytes.indexOf(hashCode.asBytes(), hash)!=0){
            throw new TPSException("invalid memtable file: hash not match");
        }

        return in;
    }

    public void dump(File file) throws IOException
    {
        ByteArrayDataOutput content = ByteStreams.newDataOutput(data.size()*24);

        for (Entry<PETKey, ValueAtTime> e : data.entrySet())
        {
            content.writeInt(e.getKey().getPropertyId());
            content.writeLong(e.getKey().getEntityId());
            content.write(TimePoint.IO.encode(e.getKey().getTime()));
            content.write(TimePoint.IO.encode(e.getKey().getTime()));
            content.writeInt(e.getValue().length());
            content.write(e.getValue().encode());
        }

        byte[] contentBytes = content.toByteArray();
        HashCode hash = Hashing.md5().hashBytes(contentBytes);

        FileChannel channel = new FileOutputStream(file).getChannel();
        MappedByteBuffer out = channel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);

        out.putInt(fileHeader.length());
        out.put(fileHeader.getBytes());
        out.putLong(System.currentTimeMillis());
        out.put(hash.asBytes());
        out.putInt(contentBytes.length);
        out.put(contentBytes);

        out.force();
        channel.force(true);
        channel.close();
    }

    public Map<Integer, NavigableMap<PETKey, ValueAtTime>> allPropertyData()
    {
        Map<Integer, NavigableMap<PETKey, ValueAtTime>> result = new HashMap<>();
        for(Integer propertyId : propertySize.keySet()){
            PETKey start = new PETKey(propertyId, 0, TimePoint.INIT);
            PETKey end   = new PETKey(propertyId, Long.MAX_VALUE, TimePoint.NOW);
            NavigableMap<PETKey, ValueAtTime> sub = data.subMap(start, true, end, true);
            result.put(propertyId, sub);
        }
        return result;
    }

    private static class MemTimePointValueIterator implements PeekingIterator<TimePointValueEntry>
    {
        private final Iterator<Entry<PETKey, ValueAtTime>> iter;
        private final int propertyId;
        private final long entityId;
        private TimePointValueEntry peeked;

        public MemTimePointValueIterator(NavigableMap<PETKey, ValueAtTime> map, int propertyId, long entityId)
        {
            this.iter = map.entrySet().iterator();
            this.peeked = findNext();
            this.entityId = entityId;
            this.propertyId = propertyId;
        }

        public MemTimePointValueIterator(NavigableMap<PETKey, ValueAtTime> map, int propertyId, long entityId, PETKey firstKey, ValueAtTime firstVal)
        {
            this.iter = map.entrySet().iterator();
            this.peeked = new TimePointValueEntry(firstKey.getTime(), firstVal);
            this.entityId = entityId;
            this.propertyId = propertyId;
        }

        @Override
        public TimePointValueEntry peek()
        {
            if(peeked!=null)
            {
                return peeked;
            }else{
                throw new NoSuchElementException();
            }
        }

        @Override
        public boolean hasNext()
        {
            return peeked!=null;
        }

        @Override
        public TimePointValueEntry next()
        {
            if(peeked!=null)
            {
                TimePointValueEntry tmp = peeked;
                peeked = findNext();
                return tmp;
            }else{
                throw new NoSuchElementException();
            }
        }

        private TimePointValueEntry findNext()
        {
            if(iter.hasNext())
            {
                Entry<PETKey, ValueAtTime> e = iter.next();
                if(e.getKey().getEntityId()==entityId && e.getKey().getPropertyId()==propertyId)
                {
                    return new TimePointValueEntry(e.getKey().getTime(), e.getValue());
                }else{
                    return null;
                }
            }else{
                return null;
            }
        }

        @Override
        public void remove()
        {
            throw new TPSRuntimeException("operation not supported");
        }
    }

}
