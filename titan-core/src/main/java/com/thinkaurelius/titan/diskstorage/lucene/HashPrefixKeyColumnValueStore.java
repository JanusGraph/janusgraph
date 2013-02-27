package com.thinkaurelius.titan.diskstorage.lucene;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
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

    private final ByteBuffer prefixKey(ByteBuffer key) {
        try {
            MessageDigest m = MessageDigest.getInstance(algorithm);
            key.mark();
            m.update(key);
            key.reset();
            byte[] hash = m.digest();
            ByteBuffer newKey = ByteBuffer.allocate(key.remaining() + numPrefixBytes);
            for (int i = 0; i < numPrefixBytes; i++) {
                newKey.put(hash[i]);
            }
            key.mark();
            newKey.put(key);
            key.reset();
            newKey.flip();
            return newKey;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private final ByteBuffer truncateKey(ByteBuffer key) {
        key.position(key.position() + numPrefixBytes);
        return key;
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(prefixKey(key), txh);
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        KeySliceQuery prefixQuery = new KeySliceQuery(prefixKey(query.getKey()),query);
        return store.getSlice(prefixQuery, txh);
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return store.get(prefixKey(key), column, txh);
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return store.containsKeyColumn(prefixKey(key), column, txh);
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        store.mutate(prefixKey(key), additions, deletions, txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        store.acquireLock(prefixKey(key), column, expectedValue, txh);
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        final RecordIterator<ByteBuffer> keys = store.getKeys(txh);
        return new RecordIterator<ByteBuffer>() {

            @Override
            public boolean hasNext() throws StorageException {
                return keys.hasNext();
            }

            @Override
            public ByteBuffer next() throws StorageException {
                return truncateKey(keys.next());
            }

            @Override
            public void close() throws StorageException {
                keys.close();
            }
        };
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
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
