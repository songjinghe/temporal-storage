package org.act.temporalProperty.meta;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.IndexMetaData;

import java.io.File;
import java.io.IOException;
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
    private final Map<Integer, SinglePropertyStore> propertyStores = new HashMap<>();
//    private Map<Integer, TPIndex> indexMap = new HashMap<>();

    public SystemMeta(){

    }

    public SinglePropertyStore getStore(int propertyId){
        return propertyStores.get(propertyId);
    }

    public void addStore(int propertyId, SinglePropertyStore store){
        propertyStores.put(propertyId, store);
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
        indexes.put(iMeta.getProId(), iMeta);
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

    public void force(File dir) throws IOException {
        SystemMetaController.forceToDisk(dir, this);
    }

    public void initStore(File storeDir, TableCache cache) throws Throwable {
        for( PropertyMetaData pMeta : properties.values()){
            SinglePropertyStore onePropStore = new SinglePropertyStore(pMeta, storeDir, cache);
            propertyStores.put(pMeta.getPropertyId(), onePropStore);
        }
    }
}
