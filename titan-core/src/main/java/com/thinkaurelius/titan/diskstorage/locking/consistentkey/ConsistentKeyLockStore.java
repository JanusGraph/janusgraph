package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

import java.util.List;

/**
 * A wrapper that adds locking support to a {@link KeyColumnValueStore} by
 * overridding
 * {@link #acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)
 * acquireLock()} and {@link #mutate(StaticBuffer, List, List, StoreTransaction)
 * mutate()}.
 */
public class ConsistentKeyLockStore implements KeyColumnValueStore {

    /**
     * Configuration setting key for the local lock mediator prefix
     */
    public static final String LOCAL_LOCK_MEDIATOR_PREFIX_KEY = "local-lock-mediator-prefix";


    /**
     * Titan data store.
     */
    final KeyColumnValueStore dataStore;

    /**
     * Store for locks on information in {@link #dataStore}. There's no Titan
     * data in here aside from locking records.
     */
    final KeyColumnValueStore lockStore;
    final LocalLockMediator localLockMediator;
    final ConsistentKeyLockConfiguration configuration;

    /**
     * Create a 
     * @param dataStore
     */
    public ConsistentKeyLockStore(KeyColumnValueStore dataStore) {
        this.dataStore = dataStore;
        this.lockStore = null;
        this.localLockMediator = null;
        this.configuration = null;
    }

    public ConsistentKeyLockStore(KeyColumnValueStore dataStore, KeyColumnValueStore lockStore, ConsistentKeyLockConfiguration config) throws StorageException {
        Preconditions.checkNotNull(config);
        this.dataStore = dataStore;
        this.configuration = config;
        this.localLockMediator = LocalLockMediators.INSTANCE.get(config.localLockMediatorPrefix + ":" + dataStore.getName());
        this.lockStore = lockStore;
    }

    public KeyColumnValueStore getDataStore() {
        return dataStore;
    }

    public KeyColumnValueStore getLockStore() {
        return lockStore;
    }

    public LocalLockMediator getLocalLockMediator() {
        return localLockMediator;
    }

    public byte[] getRid() {
        return configuration.rid;
    }

    public int getLockRetryCount() {
        return configuration.lockRetryCount;
    }

    public long getLockExpireMS() {
        return configuration.lockExpireMS;
    }

    public long getLockWaitMS() {
        return configuration.lockWaitMS;
    }

    private StoreTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null && txh instanceof ConsistentKeyLockTransaction);
        return ((ConsistentKeyLockTransaction) txh).getWrappedTransaction();
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return dataStore.containsKey(key, getTx(txh));
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return dataStore.getSlice(query, getTx(txh));
    }

    /**
     * {@inheritDoc}
     * 
     * <p/>
     * 
     * This implementation supports locking when {@code lockStore} is non-null.  
     */
    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        if (lockStore != null) {
            ConsistentKeyLockTransaction tx = (ConsistentKeyLockTransaction) txh;
            if (!tx.isMutationStarted()) {
                tx.mutationStarted();
                tx.verifyAllLockClaims();
            }
        }
        dataStore.mutate(key, additions, deletions, getTx(txh));
    }

    /**
     * {@inheritDoc}
     * 
     * <p/>
     * 
     * This implementation supports locking when {@code lockStore} is non-null.  
     */
    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        if (lockStore != null) {
            ConsistentKeyLockTransaction tx = (ConsistentKeyLockTransaction) txh;
            if (tx.isMutationStarted())
                throw new PermanentLockingException("Attempted to obtain a lock after mutations had been persisted");
            tx.writeBlindLockClaim(this, key, column, expectedValue);
        } else {
            dataStore.acquireLock(key, column, expectedValue, getTx(txh));
        }
    }

    @Override
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return dataStore.getKeys(getTx(txh));
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        return dataStore.getLocalKeyPartition();
    }


    @Override
    public String getName() {
        return dataStore.getName();
    }

    @Override
    public void close() throws StorageException {
        dataStore.close();
        if (lockStore != null) lockStore.close();
    }
}
