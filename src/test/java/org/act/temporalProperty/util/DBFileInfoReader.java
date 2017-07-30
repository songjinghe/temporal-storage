package org.act.temporalProperty.util;

import org.act.temporalProperty.impl.FileMetaData;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.LogReader;
import org.act.temporalProperty.impl.VersionEdit;
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
    public void unStableFileInfo() throws IOException {


    }

    @Test
    public void metaFileInfo() throws IOException {
        readMetaFileContent("unstable.meta");
        readMetaFileContent("unstable.new.meta");
        readMetaFileContent("stable.meta");
        readMetaFileContent("stable.new.meta");
    }

    private void readMetaFileContent(String fileName) throws IOException {
        File metaFile = new File( this.dbDir + "/" + fileName );
        if(!metaFile.exists()){
            System.out.println("##### Warning: file not exist: "+ metaFile.getAbsolutePath());
            return;
        }
        System.out.println("################## "+fileName+" #################");
        if(!isValidMetaFile(metaFile))
        {
            System.out.println("Format Error: not an valid meta file! Unexpected file end.");
            return;
        }
        FileInputStream inputStream = new FileInputStream( metaFile );
        FileChannel channel = inputStream.getChannel();
        LogReader logReader = new LogReader( channel, null, false, 0 );
        for( Slice logRecord = logReader.readRecord(); logRecord != null; logRecord = logReader.readRecord() )
        {
            VersionEdit edit = new VersionEdit( logRecord );
            for( Map.Entry<Integer,FileMetaData> entry : edit.getNewFiles().entries() )
            {
                System.out.println(entry.getValue());
            }
        }
        inputStream.close();
        channel.close();
        System.out.println();
    }


    private boolean isValidMetaFile(File newMetaFile) throws IOException {
        String EOF = "EOF!EOF!EOF!";
        RandomAccessFile raf = new RandomAccessFile(newMetaFile, "r");
        int fileLength = (int) newMetaFile.length();
        int eofLength = EOF.length();
        if(fileLength < eofLength){
            raf.seek(0);
            byte[] bytes = new byte[fileLength];
            raf.read(bytes, 0, fileLength);
            raf.close();
            System.out.println("Warning: file end up with["+new String(bytes)+"]");
            return false;
        }else{
            raf.seek(fileLength - eofLength);// Seek to the end of file
            byte[] bytes = new byte[eofLength];
            raf.read(bytes, 0, eofLength);
            raf.close();
            String eof = new String(bytes);
            if(eof.equals(EOF)){
                return true;
            }else{
                System.out.println("Warning: file end up with["+eof+"]");
                return false;
            }
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
        Table table = new FileChannelTable( fileName, channel, TableComparator.instence(), false );
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
