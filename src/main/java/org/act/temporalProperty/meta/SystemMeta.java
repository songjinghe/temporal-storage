package org.act.temporalProperty.meta;

import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.IndexMetaData;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by song on 2018-01-17.
 */
public class SystemMeta {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock( false );
    public final Condition waitWriteCondition = lock.writeLock().newCondition();
    public final Condition writeDiskComplete = lock.writeLock().newCondition();

    private final Map<Integer, PropertyMetaData> properties = new HashMap<>();
    private final Set<IndexMetaData> indexes = new HashSet<>();
    private final Map<Integer, SinglePropertyStore> propertyStores = new HashMap<>();
    private TableCache cache;
    private File dbDir;

    public SystemMeta(){

    }

    public SinglePropertyStore getStore(int propertyId){
        SinglePropertyStore store = propertyStores.get(propertyId);
        if(store==null){
            throw new TPSRuntimeException("no such property id: "+propertyId+". should create first!");
        }
        return store;
    }

    public Map<Integer, SinglePropertyStore> proStores(){return propertyStores;}

    public void addStore(int propertyId, SinglePropertyStore store){
        propertyStores.put(propertyId, store);
    }

    public Map<Integer, PropertyMetaData> getProperties() {
        return properties;
    }

    public Set<IndexMetaData> getIndexes() {
        return indexes;
    }

    public void addIndex(IndexMetaData iMeta) {
        indexes.add(iMeta);
    }

    public void addProperty(PropertyMetaData pMeta) {
        properties.put(pMeta.getPropertyId(), pMeta);
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
        this.dbDir = storeDir;
        this.cache = cache;
        for( PropertyMetaData pMeta : properties.values()){
            SinglePropertyStore onePropStore = new SinglePropertyStore(pMeta, storeDir, cache);
            propertyStores.put(pMeta.getPropertyId(), onePropStore);
        }
    }

    @Override
    public String toString() {
        return "SystemMeta{" +
                "properties=" + properties +
                ", indexes=" + indexes +
                ", dbDir=" + dbDir +
                '}';
    }
}
