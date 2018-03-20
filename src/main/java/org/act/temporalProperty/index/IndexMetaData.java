package org.act.temporalProperty.index;

import java.util.List;

/**
 * Created by song on 2018-01-17.
 */
public abstract class IndexMetaData {
    private static int nextMetaId=0;
    private int metaId;
    private IndexType type;
    private int timeStart;
    private int timeEnd;

    private IndexMetaData(IndexType type, int timeStart, int timeEnd) {
        this.metaId = nextMetaId++;
        this.type = type;
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
    }

    public int getId() {
        return metaId;
    }

    public IndexType getType() {
        return type;
    }

    public int getTimeStart() {
        return timeStart;
    }

    public int getTimeEnd() {
        return timeEnd;
    }

    /**
     * Created by song on 2018-03-19.
     */
    public static class MultiValIndexMeta extends IndexMetaData {
        private List<Integer> propertyIdList;

        public MultiValIndexMeta(List<Integer> propertyIdList, int timeStart, int timeEnd){
            super(IndexType.VALUE, timeStart, timeEnd);
        }

        public List<Integer> getPropertyIdList() {
            return propertyIdList;
        }
    }

    public static class SingleValIndexMeta extends IndexMetaData{
        private int proId;

        SingleValIndexMeta(int proId, int timeStart, int timeEnd) {
            super(IndexType.VALUE, timeStart, timeEnd);
        }

        public int getProId() {
            return proId;
        }
    }

    public static class AggrIndexMeta extends IndexMetaData{
        public AggrIndexMeta(int proId, int timeStart, int timeEnd){
            super(IndexType.TIME_AGGR, timeStart, timeEnd);
        }
    }
}
