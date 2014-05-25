package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.util.List;
import java.util.Map;

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
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException("Cannot lock on a read-only store");
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyQuery, StoreTransaction txh) throws StorageException {
        return store.getKeys(keyQuery, txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws StorageException {
        return store.getKeys(columnQuery, txh);
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException("Cannot mutate a read-only store");
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return store.getSlice(query, txh);
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        return store.getSlice(keys, query, txh);
    }
}
