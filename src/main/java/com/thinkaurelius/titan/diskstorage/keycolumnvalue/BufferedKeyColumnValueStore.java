package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BufferedKeyColumnValueStore implements KeyColumnValueStore {
    
    private final KeyColumnValueStore store;
    private final boolean bufferEnabled;
    
    public BufferedKeyColumnValueStore(KeyColumnValueStore store, boolean bufferEnabled) {
        Preconditions.checkNotNull(store);
        this.store=store;
        this.bufferEnabled=bufferEnabled;
    }

    private final StoreTransaction getTx(StoreTransaction txh) {
        assert txh instanceof BufferTransaction;
        return ((BufferTransaction)txh).getWrappedTransactionHandle();
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(key,getTx(txh));
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, int limit, StoreTransaction txh) throws StorageException {
        return store.getSlice(key,columnStart,columnEnd,limit,getTx(txh));
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, StoreTransaction txh) throws StorageException {
        return store.getSlice(key,columnStart,columnEnd,getTx(txh));
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return store.get(key,column,getTx(txh));
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return store.containsKeyColumn(key, column, getTx(txh));
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        if (bufferEnabled) {
            assert txh instanceof BufferTransaction;
            ((BufferTransaction)txh).mutate(store.getName(), key, additions, deletions);
        } else {
            store.mutate(key,additions,deletions,getTx(txh));
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        store.acquireLock(key,column,expectedValue,getTx(txh));
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return store.getKeys(getTx(txh));
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        return store.getLocalKeyPartition();
    }


    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void close() throws StorageException {
        store.close();
    }
}
