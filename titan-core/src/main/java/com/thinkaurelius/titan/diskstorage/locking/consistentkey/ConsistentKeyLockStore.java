package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
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
    
    final ConsistentKeyLocker locker;
    
    /**
     * Create a 
     * @param dataStore
     */
    public ConsistentKeyLockStore(KeyColumnValueStore dataStore, ConsistentKeyLocker locker) {
        this.dataStore = dataStore;
        this.locker = locker;
    }

    public KeyColumnValueStore getDataStore() {
        return dataStore;
    }

    private StoreTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        Preconditions.checkArgument(txh instanceof ConsistentKeyLockTransaction);
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
        if (locker != null) {
            ConsistentKeyLockTransaction tx = (ConsistentKeyLockTransaction) txh;
            if (!tx.isMutationStarted()) {
                tx.mutationStarted();
                locker.checkLocks(tx.getConsistentTransaction());
                tx.checkExpectedValues();
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
        if (locker != null) {
            ConsistentKeyLockTransaction tx = (ConsistentKeyLockTransaction) txh;
            if (tx.isMutationStarted())
                throw new PermanentLockingException("Attempted to obtain a lock after mutations had been persisted");
            KeyColumn lockID = new KeyColumn(key, column);
            locker.writeLock(lockID, tx.getConsistentTransaction());
            tx.lockedOn(this);
            tx.storeExpectedValue(this, lockID, expectedValue);
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
        // TODO close locker?
    }
    
    void deleteLocks(ConsistentKeyLockTransaction tx) throws StorageException {
        locker.deleteLocks(tx.getConsistentTransaction());
    }
}
