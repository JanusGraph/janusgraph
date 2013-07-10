package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StoreTransaction} that supports locking via
 * {@link LocalLockMediator} and writing and reading lock records in a
 * {@link ConsistentKeyLockStore}.
 * 
 * <p>
 * <b>This class is not safe for concurrent use by multiple threads.
 * Multithreaded access must be prevented or externally synchronized.</b>
 * 
 */
public class ConsistentKeyLockTransaction implements StoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyLockTransaction.class);

    /**
     * This variable starts false.  It remains false during the
     * locking stage of a transaction.  It is set to true at the
     * beginning of the first mutate/mutateMany call in a transaction
     * (before performing any writes to the backing store).
     */
    private boolean isMutationStarted;

    private final StoreTransaction baseTx;
    private final StoreTransaction consistentTx;
    
    private final Map<ConsistentKeyLockStore, Map<KeyColumn, StaticBuffer>> lockedOnStores =
            new HashMap<ConsistentKeyLockStore, Map<KeyColumn, StaticBuffer>>();

    public ConsistentKeyLockTransaction(StoreTransaction baseTx, StoreTransaction consistentTx) {
        Preconditions.checkArgument(consistentTx.getConsistencyLevel() == ConsistencyLevel.KEY_CONSISTENT);
        this.baseTx = baseTx;
        this.consistentTx = consistentTx;
    }

    StoreTransaction getBaseTransaction() {
        return baseTx;
    }
    
    StoreTransaction getConsistentTransaction() {
        return consistentTx;
    }
    
    private void lockedOn(ConsistentKeyLockStore store) {
        Map<KeyColumn, StaticBuffer> m = lockedOnStores.get(store);
        
        if (null == m) {
            m = new HashMap<KeyColumn, StaticBuffer>();
            lockedOnStores.put(store, m);
        }
    }
    
    void storeExpectedValue(ConsistentKeyLockStore store, KeyColumn lockID, StaticBuffer value) {
        Preconditions.checkNotNull(store);
        Preconditions.checkNotNull(lockID);

        lockedOn(store);
        Map<KeyColumn, StaticBuffer> m = lockedOnStores.get(store);
        assert null != m;
        if (m.containsKey(lockID)) {
            log.debug("Multiple expected values for {}: keeping initial value {} and discarding later value {}",
                    new Object[] { lockID, m.get(lockID), value });
        } else {
            m.put(lockID, value);
            log.debug("Store expected value for {}: {}", lockID, value);
        }
    }
    
    void checkExpectedValues() throws StorageException {
        for (final ConsistentKeyLockStore store : lockedOnStores.keySet()) {
            final Map<KeyColumn, StaticBuffer> m = lockedOnStores.get(store);
            for (final KeyColumn kc : m.keySet()) {
                
                final StaticBuffer ev = m.get(kc);
                
                KeySliceQuery ksq = new KeySliceQuery(kc.getKey(), kc.getColumn(), ByteBufferUtil.nextBiggerBuffer(kc.getColumn()));
                List<Entry> actualEntries = store.getSlice(ksq, this); // TODO make this consistent/QUORUM?
                
                if (null == actualEntries)
                    actualEntries = ImmutableList.<Entry>of();
                
                Iterable<StaticBuffer> avList = Iterables.transform(actualEntries, new Function<Entry, StaticBuffer>() {
                    @Override
                    public StaticBuffer apply(Entry e) {
                        assert null != e.getColumn();
                        assert e.getColumn().equals(kc.getColumn());
                        return e.getValue();
                    }
                });
                
                final Iterable<StaticBuffer> evList;
                
                if (null == ev) {
                    evList = ImmutableList.<StaticBuffer>of();
                } else {
                    evList = ImmutableList.<StaticBuffer>of(ev);
                }
                
                if (!Iterables.elementsEqual(evList, avList)) {
                    throw new PermanentLockingException(
                        "Expected value mismatch for " + kc + ": expected="
                                +  evList + " vs actual=" + avList + " (store=" + store.getName() + ")");
                }
            }
        }
    }
    
    private void deleteAllLocks() throws StorageException {
        for (ConsistentKeyLockStore s : lockedOnStores.keySet()) {
            s.deleteLocks(this);
        }
    }

    @Override
    public void rollback() throws StorageException {
        deleteAllLocks();
        baseTx.rollback();
    }

    @Override
    public void commit() throws StorageException {
        deleteAllLocks();
        baseTx.commit();
    }

    @Override
    public void flush() throws StorageException {
        baseTx.flush();
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return baseTx.getConsistencyLevel();
    }

    /**
     * Tells whether this transaction has been used in a
     * {@link ConsistentKeyLockStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     * call. When this returns true, the transaction is no longer allowed in
     * calls to
     * {@link ConsistentKeyLockStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}.
     *  
     * @return False until
     *         {@link ConsistentKeyLockStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     *         is called on this transaction instance. Returns true forever
     *         after.
     */
    public boolean isMutationStarted() {
        return isMutationStarted;
    }

    /**
     * Signals the transaction that it has been used in a call to
     * {@link ConsistentKeyLockStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     * . This transaction can't be used in subsequent calls to
     * {@link ConsistentKeyLockStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * .
     * <p>
     * Calling this method at the appropriate time is handled automatically by
     * {@link ConsistentKeyLockStore}. Titan users don't need to call this
     * method by hand.
     */
    public void mutationStarted() {
        isMutationStarted = true;
    }
}
