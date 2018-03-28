package org.act.temporalProperty.table;

import java.util.Map.Entry;
import java.util.Comparator;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.helper.DebugAssertTimeIncIterator;
import org.act.temporalProperty.helper.EqualValFilterIterator;
import org.act.temporalProperty.helper.InvalidEntityFilterIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.util.Slice;

/**
 * 将相邻Level的数据（如某文件及其Buffer）合并，并组成统一的Iterator。
 * 注意：不会删除有delete标记的record
 * rewrite by sjh at 2018-3-27
 */
public class TwoLevelMergeIterator extends AbstractIterator<Entry<Slice,Slice>> implements SeekingIterator<Slice,Slice>
{
    private SeekingIterator<Slice,Slice> latest;
    private SeekingIterator<Slice,Slice> old;
    private Comparator<Slice> comparator;

    public TwoLevelMergeIterator(SeekingIterator<Slice,Slice> latest, SeekingIterator<Slice,Slice> old, Comparator<Slice> comparator )
    {
//        this.latest = new DebugAssertTimeIncIterator(latest);
//        this.old = new DebugAssertTimeIncIterator(old);
        this.latest = latest;
        this.old = old;
        this.comparator = comparator;
    }

    @Override
    protected Entry<Slice, Slice> computeNext() {
        while (latest.hasNext() && old.hasNext()){
            Entry<Slice,Slice> mem = latest.peek();
            Entry<Slice,Slice> disk = old.peek();

            if (comparator.compare(disk.getKey(), mem.getKey()) < 0){
                return old.next();
            } else {
                InternalKey memKey = new InternalKey(mem.getKey());
                if(memKey.getValueType()==ValueType.UNKNOWN){
                    latest.next();
                } else {
                    Entry<Slice, Slice> tmp = latest.next();
                    delOld();
                    return tmp;
                }
            }
        }
        if (latest.hasNext()){ // diskIter run out
            return latest.next();
        } else if (old.hasNext()){ // memIter run out
            return old.next();
        } else{ // both ran out
            return endOfData();
        }
    }

    private void delOld() {
        if(latest.hasNext()){
            Slice until = latest.peek().getKey();
            while(old.hasNext()){
                if(comparator.compare(old.peek().getKey(), until)<0){
                    old.next();
                }
            }
        }
    }

    @Override
    public void seekToFirst()
    {
        this.latest.seekToFirst();
        this.old.seekToFirst();
    }

    @Override
    public void seek( Slice targetKey )
    {
        this.latest.seek( targetKey );
        this.old.seek( targetKey );
    }

    public static SeekingIterator<Slice,Slice> merge(SeekingIterator<Slice,Slice> latest, SeekingIterator<Slice,Slice> old){
        return new TwoLevelMergeIterator(latest, old, TableComparator.instance());
    }

    public static SeekingIterator<Slice,Slice> toDisk(SeekingIterator<Slice,Slice> latest, SeekingIterator<Slice,Slice> old){
        return new InvalidEntityFilterIterator(new EqualValFilterIterator(new TwoLevelMergeIterator(latest, old, TableComparator.instance())));
    }
}
