package org.act.temporalProperty.util;

import org.act.temporalProperty.impl.FileMetaData;

public interface FileChangeMonitor
{
    public void addFile( int i, FileMetaData targetMetaData );

    public void deleteFile( int i, FileMetaData metaData );
    
    public static class Adapter implements FileChangeMonitor
    {

        @Override
        public void addFile( int i, FileMetaData targetMetaData )
        {
            // ignore
        }

        @Override
        public void deleteFile( int i, FileMetaData metaData )
        {
            // ignore
        }
        
    }
}
