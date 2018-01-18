package org.act.temporalProperty.meta;

import org.act.temporalProperty.impl.LogReader;
import org.act.temporalProperty.impl.LogWriter;
import org.act.temporalProperty.impl.Logs;
import org.act.temporalProperty.impl.index.IndexMetaData;
import org.act.temporalProperty.meta.PropertyMetaData;
import org.act.temporalProperty.meta.PropertyMetaDataController;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 2018-01-17.
 */
public class SystemMeta {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock( false );
    private final Map<Integer, PropertyMetaData> properties = new HashMap<>();
    private final Map<Integer, IndexMetaData> indexes = new HashMap<>();
//    private Map<Integer, TemporalPropertyController> tpMap = new HashMap<>();
//    private Map<Integer, TPIndex> indexMap = new HashMap<>();

    public SystemMeta(){

    }

    public Map<Integer, PropertyMetaData> getProperties() {
        return properties;
    }

    public Map<Integer, IndexMetaData> getIndexes() {
        return indexes;
    }

    public void addProperty(PropertyMetaData pMeta) {
        properties.put(pMeta.getPropertyId(), pMeta);
    }

    public void addIndex(IndexMetaData iMeta) {
        indexes.put(iMeta.getId(), iMeta);
    }

    public void lockShared(){
        lock.readLock().lock();
    }

    public void unLockShared(){
        lock.readLock().unlock();
    }

    public void lockExclusive(){
        lock.writeLock().lock();
    }

    public void unLockExclusive(){
        lock.writeLock().unlock();
    }
}
