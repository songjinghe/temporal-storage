package org.act.temporalProperty.util;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.table.TwoLevelMergeIterator;
import org.act.temporalProperty.table.TableComparator;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * 测试基于优先队列的merge迭代器和基于代码判断的merge迭代器哪个更快
 * 目前看来后者更快。
 * Created by song on 2018-03-27.
 */
public class MergePerformanceComparison {
    private static Logger log = LoggerFactory.getLogger(MergePerformanceComparison.class);
    private String dbDir(){
        if(SystemUtils.IS_OS_WINDOWS){
            return "temporal.property.test";
        }else{
            return "/tmp/temporal.property.test";
        }
    }

    @Test
    public void compare(){
        List<List<Integer>> data = generateData(7);
        log.info("data generated.");

        List<SeekingIterator<Slice,Slice>> iteratorList4MergingIterator = generateIters(data);
        List<SeekingIterator<Slice,Slice>> myIters = generateIters(data);
        MergingIterator mergingIter = new MergingIterator(iteratorList4MergingIterator, TableComparator.instance());
//        AppendIterator appendIterator = new AppendIterator();
//        appendIterator.append(myIters.get(5));
//        appendIterator.append(myIters.get(6));
//        appendIterator.append(myIters.get(3));
        TwoLevelMergeIterator my0 = new TwoLevelMergeIterator(myIters.get(0), myIters.get(1), TableComparator.instance());
        TwoLevelMergeIterator my1 = new TwoLevelMergeIterator(myIters.get(2), myIters.get(3), TableComparator.instance());
        TwoLevelMergeIterator my2 = new TwoLevelMergeIterator(myIters.get(4), myIters.get(5), TableComparator.instance());
        TwoLevelMergeIterator my3 = new TwoLevelMergeIterator(my0, myIters.get(6), TableComparator.instance());
        TwoLevelMergeIterator my4 = new TwoLevelMergeIterator(my1, my2, TableComparator.instance());
        TwoLevelMergeIterator myIter = new TwoLevelMergeIterator(my3, my4, TableComparator.instance());
        myIter = new TwoLevelMergeIterator(myIter, myIters.get(4), TableComparator.instance());
        log.info("iterators assembled");

        List<Entry<Slice, Slice>> rMerging = new ArrayList<>();
        log.info("time {}", System.currentTimeMillis());
        while(mergingIter.hasNext()){
            rMerging.add(mergingIter.next());
        }
        log.info("time {}", System.currentTimeMillis());
        log.info("merging iterator done");
        log.info("time {}", System.currentTimeMillis());
        List<Entry<Slice, Slice>> rMy = new ArrayList<>();
        while(myIter.hasNext()){
            rMy.add(myIter.next());
        }
        log.info("time {}", System.currentTimeMillis());
        log.info("my iterator done");

        validateResult(data, rMerging, rMy);
    }

    private void validateResult(List<List<Integer>> data, List<Entry<Slice, Slice>> rMerging, List<Entry<Slice, Slice>> rMy) {
        Set<Integer> resultSet = new HashSet<>();
        for(List<Integer> l : data){
            resultSet.addAll(l);
        }
        log.info("merging size {}, my size {}", rMerging.size(), rMy.size());
        Set<Integer> rMergingSet = new HashSet<>();
//        for(int i=0; i<rMerging.size(); i++){
//            InternalKey keyMerge = new InternalKey(rMerging.get(i).getKey());
//            InternalKey keyMy = new InternalKey(rMy.get(i).getKey());
//            rMergingSet.add(keyMy.getStartTime());
//            if(keyMerge.getStartTime()!=keyMy.getStartTime()){
//                log.debug("time not equal! {} {}", keyMerge.getStartTime(), keyMy.getStartTime() );
//            }
//        }
        if(resultSet.size()!=rMergingSet.size() && resultSet.retainAll(rMergingSet)){
            throw new RuntimeException("result not equal!");
        }
    }

    private List<List<Integer>> generateData(int listCount) {
        List<List<Integer>> result = new ArrayList<>();
        for(int i=0; i<listCount; i++){
            result.add(randomIntList());
        }
        return result;
    }

    private List<SeekingIterator<Slice, Slice>> generateIters(List<List<Integer>> data) {
        List<SeekingIterator<Slice, Slice>> result = new ArrayList<>();
        for(List<Integer> d : data){
            MemTable m = new MemTable(TableComparator.instance());
            Slice pEid = new Slice(12);
            for(Integer time : d) {
                InternalKey key = new InternalKey(pEid, time);
                m.add(key.encode(), new Slice(0));
            }
            result.add(m.iterator());
        }
        return result;
    }

    private List<Integer> randomIntList(){
        List<Integer> result = new ArrayList<>();
        Random random = new Random(System.currentTimeMillis());
        for(int i=0; i<1000000; i++){
            result.add(random.nextInt(10000000));
        }
        result.sort(Integer::compareTo);
        return result;
    }
}
