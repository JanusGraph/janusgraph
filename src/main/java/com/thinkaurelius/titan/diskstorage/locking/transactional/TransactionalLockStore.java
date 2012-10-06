package com.thinkaurelius.titan.diskstorage.locking.transactional;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TransactionalLockStore implements KeyColumnValueStore {

    private final KeyColumnValueStore store;

    public TransactionalLockStore(KeyColumnValueStore store) {
        this.store=store;
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransactionHandle txh) throws StorageException {
        return store.containsKey(key,txh);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, int limit, StoreTransactionHandle txh) throws StorageException {
        return store.getSlice(key,columnStart,columnEnd,limit,txh);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, StoreTransactionHandle txh) throws StorageException {
        return store.getSlice(key,columnStart,columnEnd,txh);
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return store.get(key,column,txh);
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return store.containsKeyColumn(key,column,txh);
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransactionHandle txh) throws StorageException {
        store.mutate(key,additions,deletions,txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransactionHandle txh) throws StorageException {
        //Do nothing since the backing store is transactional locks are implicitly held
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransactionHandle txh) throws StorageException {
        return store.getKeys(txh);
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
