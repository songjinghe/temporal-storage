/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.act.temporalProperty.table;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.act.temporalProperty.impl.CompressionType;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.Options;
import org.act.temporalProperty.query.aggr.AggregationIndexKey;
import org.act.temporalProperty.util.PureJavaCrc32C;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.Slices;
import org.act.temporalProperty.util.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 新建存储文件的Builder
 *
 */
public class TableBuilder
{
    /**
     * TABLE_MAGIC_NUMBER was picked by running
     * echo http://code.google.com/p/leveldb/ | sha1sum
     * and taking the leading 64 bits.
     */
    
    private static Logger log = LoggerFactory.getLogger( TableBuilder.class );
    
    public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;
    
    private static final int TARGET_FILE_SIZE = 2097152;

    private final int blockRestartInterval;
    private final int blockSize;
    private final int blockDataSize;
    private final CompressionType compressionType;

    private final FileChannel fileChannel;
    private final BlockBuilder dataBlockBuilder;
    private final BlockBuilder indexBlockBuilder;
    private Slice lastKey;
    private final UserComparator userComparator;

    private long entryCount;

    // Either Finish() or Abandon() has been called.
    private boolean closed;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    private boolean pendingIndexEntry;
    private BlockHandle pendingHandle;  // Handle to add to index block
    private BlockHandle preHandle;
    
    private Slice compressedOutput;

    private long position;
    
    private float blankratio;

    public TableBuilder(Options options, FileChannel fileChannel, UserComparator userComparator)
    {
        Preconditions.checkNotNull(options, "options is null");
        Preconditions.checkNotNull(fileChannel, "fileChannel is null");
        try {
            Preconditions.checkState(position == fileChannel.position(), "Expected position %s to equal fileChannel.position %s", position, fileChannel.position());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        this.fileChannel = fileChannel;
        this.userComparator = userComparator;

        blockRestartInterval = options.blockRestartInterval();
        blockSize = options.blockSize();
        this.blankratio = options.blockEmptyRatio();
        blockDataSize = (int)(blockSize*options.blockEmptyRatio());
        compressionType = options.compressionType();

        dataBlockBuilder = new BlockBuilder((int) Math.min((int)(blockSize*1.11) , TARGET_FILE_SIZE), blockRestartInterval, userComparator);

//        // with expected 50% compression
//        int expectedNumberOfBlocks = 1024;
        indexBlockBuilder = new BlockBuilder(blockSize, 1, userComparator);

        lastKey = Slices.EMPTY_SLICE;
    }

    public long getEntryCount()
    {
        return entryCount;
    }

    public long getFileSize()
            throws IOException
    {
        return position + dataBlockBuilder.currentSizeEstimate();
    }

    public void add(BlockEntry blockEntry)
            throws IOException
    {
        Preconditions.checkNotNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }

    public void add(Slice key, Slice value)
            throws IOException
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");

        Preconditions.checkState(!closed, "table is finished");

        if (entryCount > 0) {
//            assert (userComparator.compare(key, lastKey) > 0) : "key must be greater than last key";
            if(userComparator.compare(key, lastKey) <= 0){
//                throw new AssertionError("key must be greater than last key, "+new AggregationIndexKey( lastKey)+" "+new AggregationIndexKey( key));
                throw new AssertionError("key must be greater than last key, "+new InternalKey( lastKey )+" "+new InternalKey( key ));
            }
        }

        // If we just wrote a block, we can now add the handle to index block
        if (pendingIndexEntry) {
            Preconditions.checkState(dataBlockBuilder.isEmpty(), "Internal error: Table has a pending index entry but data block builder is empty");

            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            //indexBlockBuilder.add(shortestSeparator, handleEncoding);
            indexBlockBuilder.add( key, handleEncoding );
            pendingIndexEntry = false;
        }

        lastKey = key;
        entryCount++;
        dataBlockBuilder.add(key, value);

        int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
        if (estimatedBlockSize >= blockDataSize) {
            flush();
        }
    }

    private void flush()
            throws IOException
    {
        Preconditions.checkState(!closed, "table is finished");
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        Preconditions.checkState(!pendingIndexEntry, "Internal error: Table already has a pending index entry to flush");

        pendingHandle = writeBlock(dataBlockBuilder);
        pendingIndexEntry = true;
    }

    private BlockHandle writeBlock(BlockBuilder blockBuilder)
            throws IOException
    {
        // close the block
        Slice raw = blockBuilder.finish();
        
//        log.debug( "block raw length: " + raw.length() + " block data length: " + blockBuilder.currentSizeEstimate() );
        //Preconditions.checkArgument( raw.length() >= blockSize, "datablock not equal to blocksize" );
        //Preconditions.checkArgument( blockBuilder.currentSizeEstimate() <= raw.length(),"datablock's data size shold smaller than block size" );

        // attempt to compress the block
        Slice blockContents = raw;
        CompressionType blockCompressionType = CompressionType.NONE;
        if (compressionType == CompressionType.SNAPPY) {
            ensureCompressedOutputCapacity(maxCompressedLength(raw.length()));
            try {
                int compressedSize = Snappy.compress(raw.getRawArray(), raw.getRawOffset(), raw.length(), compressedOutput.getRawArray(), 0);

                // Don't use the compressed data if compressed less than 12.5%,
                if (compressedSize < raw.length() - (raw.length() / 8)) {
                    blockContents = compressedOutput.slice(0, compressedSize);
                    blockCompressionType = CompressionType.SNAPPY;
                }
            }
            catch (IOException ignored) {
                // compression failed, so just store uncompressed form
            }
        }

        // create block trailer
        BlockTrailer blockTrailer = new BlockTrailer(blockCompressionType, crc32c(blockContents, blockCompressionType));
        Slice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // create a handle to this block
        BlockHandle blockHandle = new BlockHandle(position, blockContents.length());

        // write data and trailer
        position += fileChannel.write(new ByteBuffer[] {blockContents.toByteBuffer(), trailer.toByteBuffer()});
        //this.fileChannel.force( false );
        // clean up state
        blockBuilder.reset();

        return blockHandle;
    }

    private static int maxCompressedLength(int length)
    {
        // Compressed data can be defined as:
        //    compressed := item* literal*
        //    item       := literal* copy
        //
        // The trailing literal sequence has a space blowup of at most 62/60
        // since a literal of length 60 needs one tag byte + one extra byte
        // for length information.
        //
        // Item blowup is trickier to measure.  Suppose the "copy" op copies
        // 4 bytes of data.  Because of a special check in the encoding code,
        // we produce a 4-byte copy only if the offset is < 65536.  Therefore
        // the copy op takes 3 bytes to encode, and this type of item leads
        // to at most the 62/60 blowup for representing literals.
        //
        // Suppose the "copy" op copies 5 bytes of data.  If the offset is big
        // enough, it will take 5 bytes to encode the copy op.  Therefore the
        // worst case here is a one-byte literal followed by a five-byte copy.
        // I.e., 6 bytes of input turn into 7 bytes of "compressed" data.
        //
        // This last factor dominates the blowup, so the final estimate is:
        return 32 + length + (length / 6);
    }

    public void finish()
            throws IOException
    {
        Preconditions.checkState(!closed, "table is finished");

        // flush current data block
        flush();

        // mark table as closed
        closed = true;

        //FIXME write (empty) meta index block
        BlockBuilder metaIndexBlockBuilder = new BlockBuilder(blockSize, blockRestartInterval, new BytewiseComparator());
        // TODO(postrelease): Add stats and other meta blocks
        BlockHandle metaindexBlockHandle = writeBlock(metaIndexBlockBuilder);

        // add last handle to index block
        if (pendingIndexEntry) {

            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            indexBlockBuilder.add(lastKey, handleEncoding);
            pendingIndexEntry = false;
        }

        // write index block
        BlockHandle indexBlockHandle = writeBlock(indexBlockBuilder);

        // write footer
        Footer footer = new Footer(metaindexBlockHandle, indexBlockHandle);
        Slice footerEncoding = Footer.writeFooter(footer);
        position += fileChannel.write(footerEncoding.toByteBuffer());
        this.fileChannel.force( true );
    }

    public void abandon()
    {
        Preconditions.checkState(!closed, "table is finished");
        closed = true;
    }

    public static int crc32c(Slice data, CompressionType type)
    {
        PureJavaCrc32C crc32c = new PureJavaCrc32C();
        crc32c.update(data.getRawArray(), data.getRawOffset(), data.length());
        crc32c.update(type.persistentId() & 0xFF);
        return crc32c.getMaskedValue();
    }

    public void ensureCompressedOutputCapacity(int capacity)
    {
        if (compressedOutput != null && compressedOutput.length() > capacity) {
            return;
        }
        compressedOutput = Slices.allocate(capacity);
    }
}
