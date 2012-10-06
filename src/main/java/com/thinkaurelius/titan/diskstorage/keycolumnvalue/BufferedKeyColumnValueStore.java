package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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

    private final StoreTransactionHandle getTx(StoreTransactionHandle txh) {
        assert txh instanceof BufferTransactionHandle;
        return ((BufferTransactionHandle)txh).getWrappedTransactionHandle();
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransactionHandle txh) throws StorageException {
        return store.containsKey(key,getTx(txh));
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, int limit, StoreTransactionHandle txh) throws StorageException {
        return getSlice(key,columnStart,columnEnd,limit,getTx(txh));
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, StoreTransactionHandle txh) throws StorageException {
        return getSlice(key,columnStart,columnEnd,getTx(txh));
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return get(key,column,getTx(txh));
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return containsKeyColumn(key, column, getTx(txh));
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransactionHandle txh) throws StorageException {
        if (bufferEnabled) {
            assert txh instanceof BufferTransactionHandle;
            ((BufferTransactionHandle)txh).mutate(store.getName(), key, additions, deletions);
        } else {
            store.mutate(key,additions,deletions,getTx(txh));
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransactionHandle txh) throws StorageException {
        store.acquireLock(key,column,expectedValue,getTx(txh));
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransactionHandle txh) throws StorageException {
        return getKeys(getTx(txh));
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
