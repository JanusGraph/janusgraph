package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Adds a hash prefix of configurable length to the wrapped {@link KeyColumnValueStore} to randomize the
 * position of index values on a byte-ordered key ring.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class HashPrefixKeyColumnValueStore implements KeyColumnValueStore {

    private static final String DEFAULT_ALGORITHM = "MD5";

    private final String algorithm = DEFAULT_ALGORITHM;
    private final KeyColumnValueStore store;
    private final int numPrefixBytes;


    public HashPrefixKeyColumnValueStore(KeyColumnValueStore store, int numPrefixBytes) {
        Preconditions.checkArgument(numPrefixBytes > 0 && numPrefixBytes <= 16, "Invalid number of prefix bytes. Must be in [1,16]");
        this.store = store;
        this.numPrefixBytes = numPrefixBytes;
    }

    private final StaticBuffer prefixKey(StaticBuffer key) {
        try {
            MessageDigest m = MessageDigest.getInstance(algorithm);
            for (int i=0;i<key.length();i++) m.update(key.getByte(i));
            byte[] hash = m.digest();
            byte[] newKey = new byte[numPrefixBytes+key.length()];
            for (int i = 0; i < numPrefixBytes; i++) {
                newKey[i]=hash[i];
            }
            for (int i=0;i<key.length();i++) {
                newKey[numPrefixBytes+i]=key.getByte(i);
            }
            return new StaticArrayBuffer(newKey);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private final StaticBuffer truncateKey(StaticBuffer key) {
        return key.subrange(numPrefixBytes,key.length()-numPrefixBytes);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(prefixKey(key), txh);
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        KeySliceQuery prefixQuery = new KeySliceQuery(prefixKey(query.getKey()),query);
        return store.getSlice(prefixQuery, txh);
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
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
        final RecordIterator<StaticBuffer> keys = store.getKeys(txh);
        return new RecordIterator<StaticBuffer>() {

            @Override
            public boolean hasNext() throws StorageException {
                return keys.hasNext();
            }

            @Override
            public StaticBuffer next() throws StorageException {
                return truncateKey(keys.next());
            }

            @Override
            public void close() throws StorageException {
                keys.close();
            }
        };
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
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
}
