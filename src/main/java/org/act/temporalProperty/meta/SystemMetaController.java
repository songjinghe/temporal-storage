package org.act.temporalProperty.meta;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.act.temporalProperty.exception.TPSMetaLoadFailedException;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.LogReader;
import org.act.temporalProperty.impl.LogWriter;
import org.act.temporalProperty.impl.Logs;
import org.act.temporalProperty.impl.index.IndexMetaData;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Collection;

import static org.act.temporalProperty.TemporalPropertyStore.MagicNumber;
import static org.act.temporalProperty.TemporalPropertyStore.Version;

/**
 * Created by song on 2018-01-17.
 */
public class SystemMetaController {
    public static Slice encode(SystemMeta meta){
        DynamicSliceOutput out = new DynamicSliceOutput(1024*4*256);
        encode(out, meta);
        return out.slice();
    }

    public static void encode(SliceOutput out, SystemMeta meta){
        Collection<PropertyMetaData> props = meta.getProperties().values();
        out.writeInt(props.size());
        for(PropertyMetaData p: props){
            PropertyMetaDataController.encode(out, p);
        }
        Collection<IndexMetaData> indexes = meta.getIndexes().values();
        out.writeInt(indexes.size());
        for(IndexMetaData p: indexes){
            IndexMetaDataController.encode(out, p);
        }
    }

    public static SystemMeta decode(SliceInput in){
        int count = in.readInt();
        SystemMeta meta = new SystemMeta();
        for(int i=0; i<count; i++){
            PropertyMetaData pMeta = PropertyMetaDataController.decode(in);
            meta.addProperty(pMeta);
        }
        count = in.readInt();
        for(int i=0; i<count; i++){
            IndexMetaData iMeta = IndexMetaDataController.decode(in);
            meta.addIndex(iMeta);
        }
        return meta;
    }

    public static SystemMeta decode(Slice in){
        return decode(in.input());
    }

    /**
     *
     * @param file    meta file
     * @return null if is a invalid file
     */
    public static SystemMetaFile readFromDisk(File file){
        try {
            FileInputStream inputStream = new FileInputStream(file);
            FileChannel channel = inputStream.getChannel();
            LogReader logReader = new LogReader(channel, null, false, 0);
            Slice header = logReader.readRecord();
            if (header != null) {
                SliceInput in = header.input();
                Slice rawMagic = in.readBytes(40);
                int version = in.readInt();
                long time = in.readLong();
                Slice meta = logReader.readRecord();
                channel.close();
                inputStream.close();
                if(meta!=null) {
                    SystemMetaFile sysFile = new SystemMetaFile(new String(rawMagic.getBytes()), version, time, meta);
                    if(sysFile.isValid()) return sysFile;
                    else return null;
                }else{
                    return null;
                }
            }else{
                return null;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new TPSNHException("file should exist");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void forceToDisk(File dir, SystemMeta meta) throws IOException {
        try{
            removeTmpFile(dir);
            File tmpFile = getTmpFile(dir);

            DynamicSliceOutput out = new DynamicSliceOutput(1024*1024);
            out.write(MagicNumber.getBytes());
            out.writeInt(Version);
            out.writeLong(System.currentTimeMillis());

            LogWriter writer = Logs.createMetaWriter( tmpFile );
            writer.addRecord(out.slice(), false);
            writer.addRecord(encode(meta), true);
            writer.close();

            File oldFile = new File(dir, "meta.info");
            if( oldFile.exists() ) {
                if( !oldFile.delete()) throw new IOException("can not delete old meta file");
            }
            Files.move(tmpFile.toPath(), tmpFile.toPath().resolveSibling(oldFile.getName()));
        }catch ( IOException e ){
            //FIXME
            e.printStackTrace();
            throw e;
        }
    }

    private static File getTmpFile(File dir) {
        return new File(dir, "meta.info.new");
    }

    private static void removeTmpFile(File dir) throws IOException {
        File f = getTmpFile(dir);
        if(f.exists()){
            if(!f.delete()){
                throw new IOException("can not delete tmp file");
            }
        }
    }
}
