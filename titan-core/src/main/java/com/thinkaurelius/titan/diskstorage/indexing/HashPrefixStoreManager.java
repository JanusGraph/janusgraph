package com.thinkaurelius.titan.diskstorage.indexing;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * See {@link HashPrefixKeyColumnValueStore}.
 *
 * This store manager passes through all calls to the encapsulated store manager
 * provided during construction. However, it modifies the {@link StoreFeatures}
 * provided by the encapsulated store manager to force ordered and unordered
 * scans off.
 */
public class HashPrefixStoreManager implements KeyColumnValueStoreManager {

    private final KeyColumnValueStoreManager wrapped;
    private final HashPrefixKeyColumnValueStore.HashLength hashPrefixLen;

    public HashPrefixStoreManager(KeyColumnValueStoreManager wrapped, final HashPrefixKeyColumnValueStore.HashLength prefixLen) {
        Preconditions.checkArgument(wrapped!=null && prefixLen!=null);
        this.wrapped = wrapped;
        this.hashPrefixLen = prefixLen;
    }

    @Override
    public StoreTransaction beginTransaction(TransactionHandleConfig config)
            throws StorageException {
        return wrapped.beginTransaction(config);
    }

    @Override
    public void close() throws StorageException {
        wrapped.close();
    }

    @Override
    public void clearStorage() throws StorageException {
        wrapped.clearStorage();
    }

    @Override
    public StoreFeatures getFeatures() {
        return new StandardStoreFeatures.Builder(wrapped.getFeatures()).orderedScan(false).unorderedScan(false).build();
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public KeyColumnValueStore openDatabase(String name)
            throws StorageException {
        return new HashPrefixKeyColumnValueStore(wrapped.openDatabase(name), hashPrefixLen);
    }

    @Override
    public void mutateMany(
            Map<String, Map<StaticBuffer, KCVMutation>> mutations,
            StoreTransaction txh) throws StorageException {
        wrapped.mutateMany(mutations, txh);
    }

}
