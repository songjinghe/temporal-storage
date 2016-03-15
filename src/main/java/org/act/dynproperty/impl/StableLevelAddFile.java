package org.act.dynproperty.impl;

import java.io.IOException;

import org.act.dynproperty.util.Slice;

public interface StableLevelAddFile
{
    public void addFile( FileMetaData file ) throws IOException;
    public long getNextFileNumber();
    public SeekingIterator<Slice,Slice> getlastFileIterator();
    public int getlastBoundary();
}
