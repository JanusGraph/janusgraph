package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Wraps a {@link KeyColumnValueStore} and throws exceptions when a mutation is attempted.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class ReadOnlyKeyColumnValueStore implements KeyColumnValueStore {

    protected final KeyColumnValueStore store;

    public ReadOnlyKeyColumnValueStore(KeyColumnValueStore store) {
        this.store = store;
    }

    @Override
    public void close() throws StorageException {
        store.close();
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException("Cannot lock on a read-only store");
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return store.getKeys(txh);
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
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException("Cannot mutate a read-only store");
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(key, txh);
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return store.getSlice(query, txh);
    }

}
