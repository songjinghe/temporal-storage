package org.act.temporalProperty.util;

import org.act.temporalProperty.impl.FileMetaData;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.LogReader;
import org.act.temporalProperty.impl.VersionEdit;
import org.act.temporalProperty.meta.SystemMeta;
import org.act.temporalProperty.meta.SystemMetaController;
import org.act.temporalProperty.meta.SystemMetaFile;
import org.act.temporalProperty.table.FileChannelTable;
import org.act.temporalProperty.table.Table;
import org.act.temporalProperty.table.TableComparator;
import org.act.temporalProperty.table.TableIterator;
import org.junit.Test;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * Created by song on 16-9-3.
 */
public class DBFileInfoReader
{
    private String dbDir="/tmp/amitabha/dynNode";

    @Test
    public void metaFileInfo() throws IOException {
        readMetaFileContent("meta.info");
        readMetaFileContent("meta.info.new");
    }

    private void readMetaFileContent(String fileName) throws IOException {
        System.out.println("################## "+fileName+" #################");
        SystemMetaFile file = SystemMetaController.readFromDisk(new File(dbDir, fileName));
        if(file!=null && file.isValid()){
            SystemMeta meta = SystemMetaController.decode(file.getMeta());
            System.out.println(meta);
        }else{
            System.out.println("Format Error: not an valid meta file! Unexpected file end.");
        }
    }

    @Test
    public void dbtmpFileInfo() throws IOException {
        String fileName = "000000.dbtmp";
        File metaFile = new File( this.dbDir + "/" + fileName );
        if(!metaFile.exists()){
            System.out.println("##### Warning: file not exist: "+ metaFile.getAbsolutePath());
            return;
        }
        System.out.println("################## "+fileName+" #################");
        FileInputStream inputStream = new FileInputStream( new File( this.dbDir + "/" + fileName ) );
        FileChannel channel = inputStream.getChannel();
        Table table = new FileChannelTable( fileName, channel, TableComparator.instance(), false );
        TableIterator iterator = table.iterator();
        if( !iterator.hasNext() )
        {
            System.out.println("Empty 000000.dbtmp file.");
            return;
        }
        int maxTime = Integer.MIN_VALUE;
        int minTime = Integer.MAX_VALUE;
        long size = 0;
        long recordCount = 0;
        while( iterator.hasNext() )
        {
            Map.Entry<Slice,Slice> entry = iterator.next();
            Slice key = entry.getKey();
            Slice value = entry.getValue();
            InternalKey internalKey = new InternalKey( key );
            int time = internalKey.getStartTime();
            if( time < minTime )
            {
                minTime = time;
            }
            if( time > maxTime )
            {
                maxTime = time;
            }
            size += (key.length() + value.length());
            recordCount++;
        }
        inputStream.close();
        channel.close();
        System.out.println("Size: "+ humanReadableFileSize(size)+" minTime:"+ minTime +" maxTime:"+maxTime +" record count:"+recordCount);
    }

    private String humanReadableFileSize(long size)
    {
        float oneMB = 1024*1024;
        float oneKB = 1024;
        if( size > oneMB )
        {
            return ( size / oneMB ) + "MB";
        }else if ( size > oneKB )
        {
            return ( size / oneKB ) + "KB";
        }else{
            return size + "Byte";
        }

    }
}
