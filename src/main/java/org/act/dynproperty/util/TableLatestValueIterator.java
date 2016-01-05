package org.act.dynproperty.util;

import java.util.Map.Entry;

import org.act.dynproperty.impl.InternalKey;
import org.act.dynproperty.table.Block;
import org.act.dynproperty.table.BlockEntry;
import org.act.dynproperty.table.BlockIterator;
import org.act.dynproperty.table.Table;

public class TableLatestValueIterator extends AbstractSeekingIterator<Slice, Slice>
{
    private final Table table;
    private final BlockIterator blockIterator;
    private BlockLatestValueIterator current;

    public TableLatestValueIterator(Table table, BlockIterator blockIterator)
    {
        this.table = table;
        this.blockIterator = blockIterator;
        current = null;
    }

    @Override
    protected void seekToFirstInternal()
    {
        // reset index to before first and clear the data iterator
        blockIterator.seekToFirst();
        current = null;
    }

    @Override
    protected void seekInternal(Slice targetKey)
    {
        // seek the index to the block containing the key
        blockIterator.seek(targetKey);

        // if indexIterator does not have a next, it mean the key does not exist in this iterator
        if (blockIterator.hasNext()) {
            // seek the current iterator to the key
            BlockEntry pre = null;
            if( blockIterator.peek().getKey().equals( targetKey ) )
            {
                pre = blockIterator.peek();
                blockIterator.next();
            }
            current = getNextBlock();
            if( null == current )
                current = getBlockByBlockEntry( pre );
            current.seek(targetKey);
        }
        else {
            current = null;
        }
    }

    private BlockLatestValueIterator getBlockByBlockEntry( BlockEntry entry )
    {
        Slice blockHandle = entry.getValue();
        Block dataBlock = table.openBlock(blockHandle);
        return dataBlock.latestValueIterator();
    }
        
    @Override
    protected Entry<Slice, Slice> getNextElement()
    {
        // note: it must be here & not where 'current' is assigned,
        // because otherwise we'll have called inputs.next() before throwing
        // the first NPE, and the next time around we'll call inputs.next()
        // again, incorrectly moving beyond the error.
        boolean currentHasNext = false;
        while (true) {
            if (current != null) {
                currentHasNext = current.hasNext();
            }
            if (!(currentHasNext)) {
                if (blockIterator.hasNext()) {
                    current = getNextBlock();
                }
                else {
                    break;
                }
            }
            else {
                break;
            }
        }
        if (currentHasNext) {
            Entry<Slice,Slice> toret = current.next();
            if( current.hasNext() )
                return toret;
            else
            {
                BlockLatestValueIterator preIterator;
                if( blockIterator.hasNext() )
                {
                    preIterator = peekNextBlock();
                    Entry<Slice,Slice> tempEntry = preIterator.next();
                    InternalKey preKey = new InternalKey( toret.getKey() );
                    InternalKey nextKey = new InternalKey( tempEntry.getKey() );
                    if( preKey.getId().equals( nextKey.getId() ))
                    {
                        return getNextElement();
                    }
                    else
                    {
                        return toret;
                    }
                }
                else
                {
                    return toret;
                }
            }
        }
        else {
            // set current to empty iterator to avoid extra calls to user iterators
            current = null;
            return null;
        }
    }

    private BlockLatestValueIterator peekNextBlock()
    {
        BlockEntry entry = blockIterator.peek();
        if( null == entry )
            return null;
        Slice blockHandle = entry.getValue();
        Block dataBlock = table.openBlock(blockHandle);
        return dataBlock.latestValueIterator();
    }

    private BlockLatestValueIterator getNextBlock()
    {
        BlockEntry entry = blockIterator.next();
        if( null == entry )
            return null;
        Slice blockHandle = entry.getValue();
        Block dataBlock = table.openBlock(blockHandle);
        return dataBlock.latestValueIterator();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ConcatenatingIterator");
        sb.append("{blockIterator=").append(blockIterator);
        sb.append(", current=").append(current);
        sb.append('}');
        return sb.toString();
    }
}
