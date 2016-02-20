package org.act.dynproperty.impl;

import org.act.dynproperty.util.Slice;

public interface StableLevelAddFile
{
    public void addFile( FileMetaData file );
    public long getNextFileNumber();
    public SeekingIterator<Slice,Slice> getlastFileIterator();
    public int getlastBoundary();
}
