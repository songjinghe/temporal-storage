package org.act.temporalProperty.query.aggr;

import com.google.common.base.Objects;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.meta.ValueContentType;
import org.act.temporalProperty.util.Slice;


import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Created by song on 2018-04-06.
 */
public class ValueGroupingMap {
    private final int UN_GROUPED_GID = -1;
    private final TreeMap<Slice, Integer> groupMap;
    private final IndexValueType valueType;

    public ValueGroupingMap(Comparator<Slice> comparator, IndexValueType valueType) {
        this.groupMap = new TreeMap<>(comparator);
        this.valueType = valueType;
    }

    public void point2group(Slice v, int groupId) {
        groupMap.put(v, groupId);
    }

    // min: inclusive,  max: non-included
    public void range2group(Slice min, Slice max, int groupId) {
        Entry<Slice, Integer> e = groupMap.floorEntry(max);
        if (e != null) {
            groupMap.put(max, e.getValue());
        } else {
            groupMap.put(max, UN_GROUPED_GID);
        }
        groupMap.put(min, groupId);
    }

    public int group(Slice v) {
        Entry<Slice, Integer> e = groupMap.floorEntry(v);
        if (e == null) {
            return UN_GROUPED_GID;
        } else {
            return e.getValue();
        }
    }

    public IndexValueType valueType() {
        return this.valueType;
    }

    public TreeMap<Slice, Integer> map() {
        return groupMap;
    }

//    private Slice toSlice(Slice key) {
//        switch(valueType){
//            case INT:
//                return int2Slice((Integer) key);
//            case LONG:
//                return long2Slice((Long) key);
//            case FLOAT:
//                return float2Slice((Float) key);
//            case DOUBLE:
//                return double2Slice((Double) key);
//            case STRING:
//                return str2Slice((String) key);
//            default:
//                throw new TPSNHException("invalid value type");
//        }
//    }

    public Slice int2Slice( int val ){
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

    public static Comparator<Slice> INT_CMP = Comparator.comparingInt(o -> o.getInt(0));
    public static Comparator<Slice> LONG_CMP = Comparator.comparingLong(o -> o.getLong(0));;
    public static Comparator<Slice> FLOAT_CMP = Comparator.comparingDouble(o -> o.getFloat(0));
    public static Comparator<Slice> DOUBLE_CMP = FLOAT_CMP;
    public static Comparator<Slice> STR_CMP = Comparator.naturalOrder();

    public static Comparator<Slice> getComparator(IndexValueType valueType) {
        switch(valueType){
            case INT: return INT_CMP;
            case LONG: return LONG_CMP;
            case FLOAT:
            case DOUBLE:
                return FLOAT_CMP;
            case STRING:
                return STR_CMP;
            default:
                throw new TPSNHException("invalid value type");
        }
    }

    public static class IntValueGroupMap extends ValueGroupingMap{
        public IntValueGroupMap() {
            super(Comparator.naturalOrder(), IndexValueType.INT);
        }
    }

    public static class LongValueGroupMap extends ValueGroupingMap{
        public LongValueGroupMap() {
            super(Comparator.naturalOrder(), IndexValueType.LONG);
        }
    }

    public static class FloatValueGroupMap extends ValueGroupingMap{
        public FloatValueGroupMap() {
            super(Comparator.naturalOrder(), IndexValueType.FLOAT);
        }
    }

    public static class DoubleValueGroupMap extends ValueGroupingMap{
        public DoubleValueGroupMap() {
            super(Comparator.naturalOrder(), IndexValueType.DOUBLE);
        }
    }

    public static class StringValueGroupMap extends ValueGroupingMap{
        public StringValueGroupMap() {
            super(Comparator.naturalOrder(), IndexValueType.STRING);
        }
    }
}
