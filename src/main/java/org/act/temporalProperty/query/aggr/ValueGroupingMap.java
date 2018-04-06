package org.act.temporalProperty.query.aggr;

import com.google.common.base.Objects;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Created by song on 2018-04-06.
 */
public class ValueGroupingMap<VALUE_TYPE> {
    private final int UN_GROUPED_GID = -1;
    private final TreeMap<VALUE_TYPE, Integer> groupMap;
    private final ValueContentType valueType;

    public ValueGroupingMap(Comparator<VALUE_TYPE> comparator, ValueContentType valueType) {
        this.groupMap = new TreeMap<>(comparator);
        this.valueType = valueType;
    }

    public void point2group(VALUE_TYPE v, int groupId) {
        groupMap.put(v, groupId);
    }

    // min: inclusive,  max: non-included
    public void range2group(VALUE_TYPE min, VALUE_TYPE max, int groupId) {
        Entry<VALUE_TYPE, Integer> e = groupMap.floorEntry(max);
        if (e != null) {
            groupMap.put(max, e.getValue());
        } else {
            groupMap.put(max, UN_GROUPED_GID);
        }
        groupMap.put(min, groupId);
    }

    public int group(VALUE_TYPE v) {
        Entry<VALUE_TYPE, Integer> e = groupMap.floorEntry(v);
        if (e == null) {
            return UN_GROUPED_GID;
        } else {
            return e.getValue();
        }
    }

    public ValueContentType valueType() {
        return this.valueType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueGroupingMap<?> that = (ValueGroupingMap<?>) o;
        return com.google.common.base.Objects.equal(groupMap, that.groupMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(groupMap);
    }

    public TreeMap<Slice, Integer> map() {
        TreeMap<Slice, Integer> result = new TreeMap<>(getComparator());
        for(Entry<VALUE_TYPE, Integer> entry : groupMap.entrySet()) {
            result.put(toSlice(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private Slice toSlice(VALUE_TYPE key) {
        switch(valueType){
            case INT:
                return int2Slice((Integer) key);
            case LONG:
                return long2Slice((Long) key);
            case FLOAT:
                return float2Slice((Float) key);
            case DOUBLE:
                return double2Slice((Double) key);
            case STRING:
                return str2Slice((String) key);
            default:
                throw new TPSNHException("invalid value type");
        }
    }

    private Slice int2Slice(int val){
        Slice s = new Slice(4);
        s.setInt(0, val);
        return s;
    }

    private Slice long2Slice(long val){
        Slice s = new Slice(4);
        s.setLong(0, val);
        return s;
    }

    private Slice float2Slice(float val){
        Slice s = new Slice(4);
        s.output().writeFloat(val);
        return s;
    }

    private Slice double2Slice(double val){
        Slice s = new Slice(8);
        s.output().writeDouble(val);
        return s;
    }

    private Slice str2Slice(String val){
        Slice s = new Slice(val.length());
        s.output().writeBytes(val);
        return s;
    }

    private Comparator<Slice> getComparator() {
        switch(valueType){
            case INT: return Comparator.comparingInt(o -> o.getInt(0));
            case LONG: return Comparator.comparingLong(o -> o.getLong(0));
            case FLOAT:
            case DOUBLE:
                return Comparator.comparingDouble(o -> o.getFloat(0));
            case STRING:
                return Comparator.naturalOrder();
            default:
                throw new TPSNHException("invalid value type");
        }
    }

    public static class IntValueGroupMap extends ValueGroupingMap<Integer>{
        public IntValueGroupMap() {
            super(Comparator.naturalOrder(), ValueContentType.INT);
        }
    }

    public static class LongValueGroupMap extends ValueGroupingMap<Long>{
        public LongValueGroupMap() {
            super(Comparator.naturalOrder(), ValueContentType.LONG);
        }
    }

    public static class FloatValueGroupMap extends ValueGroupingMap<Float>{
        public FloatValueGroupMap() {
            super(Comparator.naturalOrder(), ValueContentType.FLOAT);
        }
    }

    public static class DoubleValueGroupMap extends ValueGroupingMap<Double>{
        public DoubleValueGroupMap() {
            super(Comparator.naturalOrder(), ValueContentType.DOUBLE);
        }
    }

    public static class StringValueGroupMap extends ValueGroupingMap<Double>{
        public StringValueGroupMap() {
            super(Comparator.naturalOrder(), ValueContentType.STRING);
        }
    }
}
