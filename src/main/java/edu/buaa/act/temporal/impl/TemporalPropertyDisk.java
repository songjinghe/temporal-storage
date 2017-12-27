package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.*;
import edu.buaa.act.temporal.impl.table.Table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by song on 17-12-6.
 */
public class TemporalPropertyDisk
{
    // meta info
    private int id = -1;
    private TemporalPropertyType type = TemporalPropertyType.INVALID;
    private boolean isVarLength = false;
    private int length; // only valid when varLength=false;
    private TimePoint earliestTime;
    private TimePoint unstableStartTime;
    private TimePoint unstableEndTime;

    private Deque<Table> fileStack = new LinkedList<>();

    public ValueAtTime getValue(long entityId, TimePoint time) {
        return null;
    }

    public List<TimeValueEntry> getList(int entityId, TimeInterval timeRange) {
        return null;
    }

    public void getCallback(long entityId, TimePoint timePoint, TimeValueEntryCallback callback) {

    }

    public void encode(DataOutput out) throws IOException
    {
        out.writeInt(id);
        out.writeInt(type.getId());
        out.writeBoolean(isVarLength);
        out.writeInt(length);
        out.write(TimePoint.IO.encode(earliestTime));
        out.write(TimePoint.IO.encode(unstableStartTime));
        out.write(TimePoint.IO.encode(unstableEndTime));
        out.writeInt(fileStack.size());
        for(Table t : fileStack){
            t.encode(out);
        }
    }

    public static TemporalPropertyDisk decode(DataInput in) throws IOException
    {
        TemporalPropertyDisk tp = new TemporalPropertyDisk();
        tp.id = in.readInt();
        tp.type = TemporalPropertyType.getById(in.readInt());
        tp.isVarLength = in.readBoolean();
        tp.length = in.readInt();
        tp.earliestTime = TimePoint.IO.decode(in);
        tp.unstableStartTime = TimePoint.IO.decode(in);
        tp.unstableEndTime = TimePoint.IO.decode(in);
        return tp;
    }

    public int getId()
    {
        return id;
    }
}
