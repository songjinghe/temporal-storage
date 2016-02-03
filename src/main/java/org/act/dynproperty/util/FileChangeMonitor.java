package org.act.dynproperty.util;

import org.act.dynproperty.impl.FileMetaData;

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
