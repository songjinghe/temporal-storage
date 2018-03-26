package org.act.temporalProperty.util;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.TemporalPropertyStoreFactory;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Created by song on 2018-01-23.
 */
public class StoreBuilder {
    private static Logger log = LoggerFactory.getLogger(StoreBuilder.class);

    private final File dbDir;
    private final TemporalPropertyStore store;

    public StoreBuilder(String storePath, boolean fromScratch) throws Throwable {
        this.dbDir = new File(storePath);
        if(fromScratch){
            if(dbDir.exists()) {
                deleteAllFilesOfDir(dbDir);
            }
            dbDir.mkdir();
            store = TemporalPropertyStoreFactory.newPropertyStore(dbDir);
        }else{
            store = TemporalPropertyStoreFactory.newPropertyStore(dbDir);
        }
    }

    public TemporalPropertyStore store(){
        return this.store;
    }

    public static void setIntProperty(TemporalPropertyStore store, int time, long entityId, int propertyId, int value) {
        Slice id = getIdSlice(entityId, propertyId);
        InternalKey key = new InternalKey(id, time, 4, ValueType.VALUE);
        store.setProperty(key.encode(), ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
    }

    private static Slice getIdSlice(long entityId, int propertyId) {
        Slice result = new Slice(12);
        result.setLong(0, entityId);
        result.setInt(8, propertyId);
        return result;
    }

    private static void deleteAllFilesOfDir(File path) {
        if (!path.exists())
            return;
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (int i = 0; i < files.length; i++) {
            deleteAllFilesOfDir(files[i]);
        }
        path.delete();
    }
}
