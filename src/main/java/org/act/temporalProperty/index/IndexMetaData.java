package org.act.temporalProperty.index;

import java.util.List;

/**
 * Created by song on 2018-01-17.
 */
public class IndexMetaData {
    private int metaId;
    private IndexType type;
    private List<Integer> propertyIdList;
    private int timeStart;
    private int timeEnd;
    private long fileSize;

    public IndexMetaData(int id, IndexType type, List<Integer> pidList, int start, int end, long fileSize) {
        this.metaId = id;
        this.type = type;
        this.propertyIdList = pidList;
        this.timeStart = start;
        this.timeEnd = end;
        this.fileSize = fileSize;
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

    public List<Integer> getPropertyIdList(){
        return propertyIdList;
    }

    public long getFileSize() {
        return fileSize;
    }

    //    /**
//     * Created by song on 2018-03-19.
//     */
//    public static class MultiValIndexMeta extends IndexMetaData {
//        private List<Integer> propertyIdList;
//
//        public MultiValIndexMeta(List<Integer> propertyIdList, int timeStart, int timeEnd){
//            super(IndexType.MULTI_VALUE, timeStart, timeEnd);
//        }
//
//        public MultiValIndexMeta(int id, IndexType tid, List<Integer> pidList, int start, int end) {
//            super(id, tid, start, end);
//        }
//
//        public List<Integer> getPropertyIdList() {
//            return propertyIdList;
//        }
//    }
//
//    public static class SingleValIndexMeta extends IndexMetaData{
//        private int proId;
//
//        SingleValIndexMeta(int proId, int timeStart, int timeEnd) {
//            super(IndexType.SINGLE_VALUE, timeStart, timeEnd);
//            this.proId = proId;
//        }
//
//        public int getProId() {
//            return proId;
//        }
//    }
//
//    public static class AggrIndexMeta extends IndexMetaData{
//        private int proId;
//
//        public AggrIndexMeta(int proId, int timeStart, int timeEnd){
//            super(IndexType.TIME_AGGR, timeStart, timeEnd);
//            this.proId = proId;
//        }
//
//        public int getProId() {
//            return proId;
//        }
//    }
}
