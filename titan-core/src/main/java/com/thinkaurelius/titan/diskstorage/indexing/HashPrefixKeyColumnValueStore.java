package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.hash.HashCode;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.HashUtility;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Adds a hash prefix of configurable length to the wrapped {@link KeyColumnValueStore} to randomize the
 * position of index values on a byte-ordered key ring.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class HashPrefixKeyColumnValueStore implements KeyColumnValueStore {

    public enum HashLength {
        SHORT, LONG;

        public int length() {
            switch (this) {
                case SHORT: return 4;
                case LONG: return 8;
                default: throw new AssertionError("Unknown hash type: " + this);
            }
        }
    }

    private static final StaticBuffer.Factory<HashCode> SHORT_HASH_FACTORY = new StaticBuffer.Factory<HashCode>() {
        @Override
        public HashCode get(byte[] array, int offset, int limit) {
            return HashUtility.SHORT.get().hashBytes(array, offset, limit);
        }
    };

    private static final StaticBuffer.Factory<HashCode> LONG_HASH_FACTORY = new StaticBuffer.Factory<HashCode>() {
        @Override
        public HashCode get(byte[] array, int offset, int limit) {
            return HashUtility.LONG.get().hashBytes(array,offset,limit);
        }
    };

    private final KeyColumnValueStore store;
    private final HashLength hashPrefixLen;


    public HashPrefixKeyColumnValueStore(KeyColumnValueStore store, final HashLength prefixLen) {
        this.store = store;
        this.hashPrefixLen = prefixLen;
    }

    private final StaticBuffer prefixKey(StaticBuffer key) {
        return prefixKey(hashPrefixLen,key);
    }

    public static final StaticBuffer prefixKey(final HashLength hashPrefixLen, final StaticBuffer key) {
        final int prefixLen = hashPrefixLen.length();
        final StaticBuffer.Factory<HashCode> hashFactory;
        switch (hashPrefixLen) {
            case SHORT:
                hashFactory = SHORT_HASH_FACTORY;
                break;
            case LONG:
                hashFactory = LONG_HASH_FACTORY;
                break;
            default: throw new IllegalArgumentException("Unknown hash prefix: " + hashPrefixLen);
        }

        HashCode hashcode = key.as(hashFactory);
        WriteByteBuffer newKey = new WriteByteBuffer(prefixLen+key.length());
        assert prefixLen==4 || prefixLen==8;
        if (prefixLen==4) newKey.putInt(hashcode.asInt());
        else newKey.putLong(hashcode.asLong());
        newKey.putBytes(key);
        return newKey.getStaticBuffer();
    }

    private StaticBuffer truncateKey(StaticBuffer key) {
        return key.subrange(hashPrefixLen.length(), key.length() - hashPrefixLen.length());
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(prefixKey(key), txh);
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        KeySliceQuery prefixQuery = new KeySliceQuery(prefixKey(query.getKey()), query);
        return store.getSlice(prefixQuery, txh);
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        List<StaticBuffer> prefixedKeys = new ArrayList<StaticBuffer>(keys.size());

        for (StaticBuffer key : keys) {
            prefixedKeys.add(prefixKey(key));
        }

        return store.getSlice(prefixedKeys, query, txh);
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        store.mutate(prefixKey(key), additions, deletions, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        store.acquireLock(prefixKey(key), column, expectedValue, txh);
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyQuery, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException("getKeys(KeyRangeQuery, StoreTransaction) is not supported in hash prefixed mode.");
    }

    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws StorageException {
        return new PrefixedRowIterator(store.getKeys(columnQuery, txh));
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void close() throws StorageException {
        store.close();
    }

    private class PrefixedRowIterator implements KeyIterator {
        private final KeyIterator rows;

        public PrefixedRowIterator(KeyIterator rows) {
            this.rows = rows;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            return rows.getEntries();
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() {
            return truncateKey(rows.next());
        }

        @Override
        public void close() throws IOException {
            rows.close();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
