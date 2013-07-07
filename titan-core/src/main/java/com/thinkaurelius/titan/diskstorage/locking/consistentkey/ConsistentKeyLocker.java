package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

/**
 * A locker which is built on {@link KeyColumnValueStore}. This class is not
 * safe for unsynchronized access by multiple threads.
 */
public class ConsistentKeyLocker implements Locker {

    
    /**
     * Locks written by {@link #writeLock(Entry)} but not yet checked by
     * {@link #checkLocks()}. These will next either be checked by
     * {@code checkLocks()} or deleted by {@link #deleteLocks()}, whichever
     * happens first.
     */
    private final LinkedList<KeyColumn> uncheckedLocks;
    
    /**
     * Locks both written {@link #writeLock(Entry)} and also later checked by
     * {@link #checkLocks()}. These will be deleted by the next call to
     * {@link #deleteLocks()}.
     */
    private final LinkedList<KeyColumn> checkedLocks;
    
    private final ConsistentKeyLockerConfiguration conf;
    
    private static final StaticBuffer zeroBuf = ByteBufferUtil.getIntBuffer(0); // TODO this does not belong here
    
    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyLocker.class);

    public ConsistentKeyLocker(ConsistentKeyLockerConfiguration conf) {
        this.conf = conf;
        this.uncheckedLocks = new LinkedList<KeyColumn>();
        this.checkedLocks = new LinkedList<KeyColumn>();
    }

    /**
     * {@inheritDoc}
     * <p>
     * If any store operation throws {@link PermanentStorageException}, then
     * this method attempts to delete any data associated with this
     * partially-written lock from the store (logging and discarding any
     * additional exceptions that might arise during the deletion attempt), and
     * then wraps the {@code PSE} in a {@link PermanentLockingException} and
     * throws that.
     * <p>
     * If any store operation throws {@link TemporaryStorageException}, then it
     * is logged and the operation is retried up to the configured retry limit.
     * If the retry limit is exceeded, then a cleanup attempt will be made as in
     * the {@code PermanentStorageException} case, and then the a new
     * {@link TemporaryLockingExceptino} is thrown. The
     * {@code TemporaryStorageException}s thrown and caught inside this method's
     * implementation will be logged but will be otherwise invisible to the
     * caller of this method.
     */
    @Override
    public void writeLock(KeyColumn lockID, StoreTransaction txh) throws StorageException {
        
        if (lockLocally(lockID, txh)) {
            boolean ok = false;
            try {
                tryLockRemotely(lockID, txh);
                uncheckedLocks.add(lockID); // TODO thread unsafe and doesnt record txn
                ok = true;
            } catch (TemporaryStorageException tse) {
                throw new TemporaryLockingException(tse);
            } catch (AssertionError ae) {
                throw ae; // Concession to ease testing
            } catch (Throwable t) {
                throw new PermanentLockingException(t);
            } finally {
                if (!ok)
                    unlockLocally(lockID, txh);
            }
        } else {
            // Fail immediately with no retries on local contention
            throw new TemporaryLockingException("Local lock contention");
        }
    }
    
    private boolean lockLocally(KeyColumn lockID, StoreTransaction txh) {
        final LocalLockMediator<StoreTransaction> m = conf.getLocalLockMediator();
        return m.lock(lockID, txh, conf.getTimes().getApproxNSSinceEpoch(false) + conf.getLockExpireNS(), TimeUnit.NANOSECONDS);
    }
    
    private void unlockLocally(KeyColumn lockID, StoreTransaction txh) {
        final LocalLockMediator<StoreTransaction> m = conf.getLocalLockMediator();
        m.unlock(lockID, txh);
    }
    
    private void tryLockRemotely(KeyColumn lockID, StoreTransaction txh) throws Throwable {
        
        final StaticBuffer lockKey = conf.getSerializer().toLockKey(lockID.getKey(), lockID.getColumn());
        StaticBuffer oldLockCol = null;
        
        for (int i = 0; i < conf.getLockRetryCount(); i++) {
            WriteResult wr = tryWriteLockOnce(lockKey, oldLockCol, txh);
            if (wr.isSuccessful() && wr.getDurationNS() <= conf.getLockWaitNS()) {
                log.debug("Lock write succeeded {} {}", lockID, txh);
                return;
            }
            oldLockCol = wr.getLockCol();
            handleMutationFailure(lockID, lockKey, wr, txh);
        }
        tryDeleteLockOnce(lockKey, oldLockCol, txh);
        // TODO log exception or successful too-slow write here
        throw new TemporaryStorageException("Lock write retry count exceeded");
    }
    
    /**
     * Log a message and/or throw an exception in response to a lock write
     * mutation that failed. "Failed" means that the mutation either succeeded
     * but took longer to complete than
     * {@link ConsistentKeyLockerConfiguration#getLockWait(TimeUnit)}, or that
     * the call to mutate threw something.
     * 
     * @param lockID
     *            coordinates identifying the lock we tried but failed to
     *            acquire
     * @param lockKey
     *            the byte value of the key that we mutated or attempted to
     *            mutate in the lock store
     * @param wr
     *            result of the mutation
     * @param txh
     *            transaction attempting the lock
     * @throws Throwable
     *             if {@link WriteResult#getThrowable()} is not an instance of
     *             {@link TemporaryStorageException}
     */
    private void handleMutationFailure(KeyColumn lockID, StaticBuffer lockKey, WriteResult wr, StoreTransaction txh) throws Throwable {
        Throwable error = wr.getThrowable();
        if (null != error) {
            if (error instanceof TemporaryStorageException) {
                // Log error and continue the loop
                log.warn("Temporary exception during lock write", error);
            } else {
                /*
                 * Probably a PermanentStorageException or an unchecked
                 * exception.  Try to delete any previous writes and then die.
                 * Do not retry even if we have retries left.
                 */
                log.error("Fatal exception encountered during attempted lock write", error);
                WriteResult dwr = tryDeleteLockOnce(lockKey, wr.getLockCol(), txh);
                if (!dwr.isSuccessful()) {
                    log.warn("Failed to delete lock write: abandoning potentially-unreleased lock on " + lockID, dwr.getThrowable());
                }
                throw error;
            }
        } else {
            log.warn("Lock write succeeded but took too long: duration {} ms exceeded limit {} ms",
                     wr.getDuration(TimeUnit.MILLISECONDS),
                     conf.getLockWait(TimeUnit.MILLISECONDS));
        }
    }
    
    private WriteResult tryWriteLockOnce(StaticBuffer key, StaticBuffer del, StoreTransaction txh) {
        Throwable t = null;
        final long before = conf.getTimes().getApproxNSSinceEpoch(false);
        StaticBuffer newLockCol = conf.getSerializer().toLockCol(before, conf.getRid());
        Entry newLockEntry = new StaticBufferEntry(newLockCol, zeroBuf);
        try {
            conf.getStore().mutate(key, Arrays.asList(newLockEntry), null == del ? null : Arrays.asList(del), txh);
        } catch (StorageException e) {
            t = e;
        }
        final long after = conf.getTimes().getApproxNSSinceEpoch(false);
        
        return new WriteResult(before, after, newLockCol, t);
    }
    
    private WriteResult tryDeleteLockOnce(StaticBuffer key, StaticBuffer col, StoreTransaction txh) {
        Throwable t = null;
        final long before = conf.getTimes().getApproxNSSinceEpoch(false);
        try {
            conf.getStore().mutate(key, null, Arrays.asList(col), txh);
        } catch (StorageException e) {
            t = e;
        }
        final long after = conf.getTimes().getApproxNSSinceEpoch(false);
        return new WriteResult(before, after, null, t);
    }

    @Override
    public void checkLocks() throws StorageException {
        // TODO Auto-generated method stub
    }

    @Override
    public void deleteLocks() throws StorageException {
        // TODO Auto-generated method stub
    }
    
    private static class WriteResult {
        private final long beforeNS;
        private final long afterNS;
        private final StaticBuffer lockCol;
        private final Throwable throwable;
        
        public WriteResult(long beforeNS, long afterNS, StaticBuffer lockCol, Throwable throwable) {
            this.beforeNS = beforeNS;
            this.afterNS = afterNS;
            this.lockCol = lockCol;
            this.throwable = throwable;
        }
        
        public long getDurationNS() {
            return afterNS - beforeNS;
        }
        
        public long getDuration(TimeUnit tu) {
            return tu.convert(afterNS - beforeNS, TimeUnit.NANOSECONDS);
        }
        
        public boolean isSuccessful() {
            return null == throwable;
        }
        
        public StaticBuffer getLockCol() {
            return lockCol;
        }
        
        public Throwable getThrowable() {
            return throwable;
        }
    }

}
