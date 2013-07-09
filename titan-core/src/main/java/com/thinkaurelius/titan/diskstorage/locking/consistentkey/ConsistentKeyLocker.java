package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
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

     
    private final ConsistentKeyLockerConfiguration conf;
    
    private final LockState lockState;
    
    private static final StaticBuffer zeroBuf = ByteBufferUtil.getIntBuffer(0); // TODO this does not belong here
    
    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyLocker.class);

    /**
     * Create a new locker.
     * 
     * @param conf locker configuration
     */
    public ConsistentKeyLocker(ConsistentKeyLockerConfiguration conf) {
        this.conf = conf;
        this.lockState = new LockState();
    }
    
    /**
     * This is only useful in testing and may go away at any time. Use
     * {@link #ConsistentKeyLocker(ConsistentKeyLockerConfiguration)} instead of
     * this constructor.
     * 
     * @param conf
     *            locker configuration
     * @param locks
     *            this locker's internal state map
     */
    public ConsistentKeyLocker(ConsistentKeyLockerConfiguration conf, LockState locks) {
        this.conf = conf;
        this.lockState = locks;
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
        
        if (lockState.has(txh, lockID)) {
            log.warn("Transaction {} already wrote lock on {}", txh, lockID);
            return;
        }
        
        if (lockLocally(lockID, txh)) {
            boolean ok = false;
            try {
                long tsNS = tryLockRemotely(lockID, txh);
                lockLocally(lockID, tsNS, txh); // update local lock expiration time
                lockState.take(txh, lockID, new LockStatus(tsNS, TimeUnit.NANOSECONDS));
                ok = true;
            } catch (TemporaryStorageException tse) {
                throw new TemporaryLockingException(tse);
            } catch (AssertionError ae) {
                // Concession to ease testing with mocks & behavior verification
                ok = true;
                throw ae;
            } catch (Throwable t) {
                throw new PermanentLockingException(t);
            } finally {
                if (!ok) {
                    // lockState.release(txh, lockID); // has no effect
                    unlockLocally(lockID, txh);
                }
            }
        } else {
            // Fail immediately with no retries on local contention
            throw new TemporaryLockingException("Local lock contention");
        }
    }
    
    private boolean lockLocally(KeyColumn lockID, StoreTransaction txh) {
        return lockLocally(lockID, conf.getTimes().getApproxNSSinceEpoch(false), txh);
    }
    
    private boolean lockLocally(KeyColumn lockID, long tsNS, StoreTransaction txh) {
        final LocalLockMediator<StoreTransaction> m = conf.getLocalLockMediator();
        return m.lock(lockID, txh, tsNS + conf.getLockExpireNS(), TimeUnit.NANOSECONDS);
    }
    
    private void unlockLocally(KeyColumn lockID, StoreTransaction txh) {
        final LocalLockMediator<StoreTransaction> m = conf.getLocalLockMediator();
        m.unlock(lockID, txh);
    }
    
    /**
     * Try to write a lock record remotely up to
     * {@link conf#getLockRetryCount()} times. If the store produces
     * {@link TemporaryLockingException}, then we'll call mutate again to add a
     * new column with an updated timestamp and to delete the column that tried
     * to write when the store threw an exception. We continue like that up to
     * the retry limit. If the store throws anything else, such as an unchecked
     * exception or a {@link PermanentStorageException}, then we'll try to
     * delete whatever we added and return without further retries.
     * 
     * @param lockID
     *            lock to acquire
     * @param txh
     *            transaction
     * @return the timestamp, in nanoseconds since UNIX Epoch, on the lock
     *         column that we successfully wrote to the store
     * @throws TemporaryLockingException
     *             if the lock retry count is exceeded without successfully
     *             writing the lock in less than the wait limit
     * @throws Throwable
     *             if the storage layer throws anything else
     */
    private long tryLockRemotely(KeyColumn lockID, StoreTransaction txh) throws Throwable {
        
        final StaticBuffer lockKey = conf.getSerializer().toLockKey(lockID.getKey(), lockID.getColumn());
        StaticBuffer oldLockCol = null;
        
        for (int i = 0; i < conf.getLockRetryCount(); i++) {
            WriteResult wr = tryWriteLockOnce(lockKey, oldLockCol, txh);
            if (wr.isSuccessful() && wr.getDurationNS() <= conf.getLockWaitNS()) {
                log.debug("Lock write succeeded {} {}", lockID, txh);
                return wr.getBeforeNS();
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
                 * A PermanentStorageException or an unchecked exception. Try to
                 * delete any previous writes and then die. Do not retry even if
                 * we have retries left.
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
    public void checkLocks(StoreTransaction tx) throws StorageException {
        Map<KeyColumn, LockStatus> m = lockState.getLocksForTx(tx);
        
        if (m.isEmpty()) {
            return; // no locks for this tx
        }

        // We never receive interrupts in normal operation; one can only appear
        // during Thread.sleep(), and in that case it probably means the entire
        // Titan process is shutting down; for this reason, we return ASAP on an
        // interrupt
        try {
            for (KeyColumn kc : m.keySet()) {
                checkSingleLock(kc, m.get(kc), tx);
            }
        } catch (InterruptedException e) {
           throw new TemporaryLockingException(e);
        }
    }
    
    private void checkSingleLock(final KeyColumn kc, final LockStatus ls, final StoreTransaction tx) throws StorageException, InterruptedException {
        
        if (ls.isChecked())
            return;

        // Sleep, if necessary
        // We could be smarter about sleeping by iterating oldest -> latest...
        final long nowNS = conf.getTimes().sleepUntil(ls.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS));

        // Slice the store
        KeySliceQuery ksq = new KeySliceQuery(conf.getSerializer().toLockKey(kc.getKey(), kc.getColumn()), ByteBufferUtil.zeroBuffer(9), ByteBufferUtil.oneBuffer(9));
        List<Entry> claimEntries = tryCheckGetSlice(ksq, tx);
        
        // Extract timestamp and rid from the column in each returned Entry...
        Iterable<TimestampRid> iter = Iterables.transform(claimEntries, new Function<Entry, TimestampRid>() {
            @Override
            public TimestampRid apply(Entry e) {
                return conf.getSerializer().fromLockColumn(e.getColumn());
            }
        });
        // ...and then filter out the TimestampRid objects with expired timestamps
        iter = Iterables.filter(iter, new Predicate<TimestampRid>() {
            @Override
            public boolean apply(TimestampRid tr) {
                if (tr.getTimestamp() < nowNS - conf.getLockExpire(TimeUnit.NANOSECONDS)) {
                    log.warn("Discarded expired claim on {} with timestamp {}", kc, tr.getTimestamp());
                    return false;
                }
                return true;
            }
        });
        
       checkSeniority(kc, ls, iter);
       ls.setChecked();
    }
    
    private List<Entry> tryCheckGetSlice(KeySliceQuery ksq, StoreTransaction tx) throws StorageException {
        
        for (int i = 0; i < conf.getLockRetryCount(); i++) {
            // TODO either make this like writeLock so that it handles all Throwable types (and pull that logic out into a shared method) or make writeLock like this in that it only handles Temporary/PermanentSE
            try {
                return conf.getStore().getSlice(ksq, tx);
            } catch (PermanentStorageException e) {
                log.error("Failed to check locks", e);
                throw new PermanentLockingException(e);
            } catch (TemporaryStorageException e) {
                log.warn("Temporary storage failure while checking locks", e);
            }
        }
        
        throw new TemporaryStorageException("Maximum retries (" + conf.getLockRetryCount() + ") exceeded while checking locks");
    }

    private void checkSeniority(KeyColumn target, LockStatus ls, Iterable<TimestampRid> claimTRs) throws StorageException {
        
        int trCount = 0;
        
        for (TimestampRid tr : claimTRs) {
            trCount++;
            
            if (!conf.getRid().equals(tr.getRid())) {
                final String msg =  "Lock on " + target + " already held by " + tr.getRid() + " (we are " + conf.getRid() +")";
                log.debug(msg);
                throw new TemporaryLockingException(msg);
            }
           
            if (tr.getTimestamp() == ls.getWrittenTimestamp(TimeUnit.NANOSECONDS)) {
                log.debug("Lock check succeeded for {}", target);
                return;
            }

            log.warn("Skipping outdated lock on {} with our rid ({}) but mismatched timestamp (actual ts {}, expected ts {})",
                     new Object[] { target, tr.getRid(), tr.getTimestamp(),
                                    ls.getWrittenTimestamp(TimeUnit.NANOSECONDS) });
        }

        /*
         * Both exceptions below shouldn't happen under normal operation with a
         * sane configuration. When they are thrown, they have one of two likely
         * root causes:
         * 
         * 1. Due to a problem with this locker's store configuration or the
         * store itself, this locker's store "lost" a write. Specifically, a
         * column previously added to the store by writeLock(...) was not
         * returned on a subsequent read by checkLocks(...). The precise root
         * cause is store-specific. With Cassandra, for instance, this problem
         * could arise if the locker is configured to talk to Cassandra at a
         * consistency level below QUORUM.
         * 
         * 2. One of our previously written locks has already expired by the
         * time we tried to read it.
         * 
         * There might be additional causes that haven't occurred to me, but
         * these two seem most likely.
         */
        if (0 == trCount) {
            throw new PermanentLockingException("No lock columns found for " + target);
        } else {
            final String msg = "Read "
                    + trCount
                    + " locks with our rid "
                    + conf.getRid()
                    + " but mismatched timestamps; no lock column contained our timestamp ("
                    + ls.getWrittenTimestamp(TimeUnit.NANOSECONDS) + ")";
            throw new PermanentStorageException(msg);
        }
    }

    @Override
    public void deleteLocks(StoreTransaction tx) throws StorageException {
        Map<KeyColumn, LockStatus> m = lockState.getLocksForTx(tx);
        
        for (KeyColumn kc : m.keySet()) {
            LockStatus ls = m.get(kc);
            tryDeleteSingleLock(kc, ls, tx);
            // Regardless of whether we successfully deleted the lock from storage, take it out of the local mediator
            conf.getLocalLockMediator().unlock(kc, tx);
            lockState.release(tx, kc);
        }
    }
    
    private void tryDeleteSingleLock(KeyColumn kc, LockStatus ls, StoreTransaction tx) throws StorageException {
        List<StaticBuffer> dels = ImmutableList.of(conf.getSerializer().toLockCol(ls.getWrittenTimestamp(TimeUnit.NANOSECONDS), conf.getRid()));
        for (int i = 0; i < conf.getLockRetryCount(); i++) {
            try {
                conf.getStore().mutate(conf.getSerializer().toLockKey(kc.getKey(), kc.getColumn()), null, dels, tx);
                return;
            } catch (TemporaryStorageException e) {
                log.warn("Temporary storage exception while deleting lock", e);
                // don't return -- iterate and retry
            } catch (PermanentStorageException e) {
                log.error("Permanent storage exception while deleting lock", e);
                return; // give up on this lock, but try the next one
            }
        } 
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
        
        public long getBeforeNS() {
            return beforeNS;
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
    
    public static class LockStatus {
        
        private long writtenNS;
        private boolean checked;
        
        public LockStatus(long writtenTimestamp, TimeUnit tu) {
            this.writtenNS = TimeUnit.NANOSECONDS.convert(writtenTimestamp, tu);
            this.checked = false;
        }

        public long getWrittenTimestamp(TimeUnit tu) {
            return tu.convert(writtenNS, tu);
        }

        public void setWrittenTimestamp(long ts, TimeUnit tu) {
            this.writtenNS = TimeUnit.NANOSECONDS.convert(ts, tu);
        }

        public boolean isChecked() {
            return checked;
        }

        public void setChecked() {
            this.checked = true;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (checked ? 1231 : 1237);
            result = prime * result + (int) (writtenNS ^ (writtenNS >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            LockStatus other = (LockStatus) obj;
            if (checked != other.checked)
                return false;
            if (writtenNS != other.writtenNS)
                return false;
            return true;
        }
    }
    
    public static class LockState {

        
        /**
         * Locks taken in the LocalLockMediator and written to the store (but not
         * necessarily checked)
         */
        private final ConcurrentMap<StoreTransaction, Map<KeyColumn, LockStatus>> locks;
        
        public LockState() {
            // TODO this wild guess at the concurrency level should not be hardcoded
            this(new MapMaker().concurrencyLevel(8).weakKeys()
                    .<StoreTransaction, Map<KeyColumn, LockStatus>>makeMap());
        }
        
        public LockState(ConcurrentMap<StoreTransaction, Map<KeyColumn, LockStatus>> locks) {
            this.locks = locks;
        }
        
        public boolean has(StoreTransaction tx, KeyColumn kc) {
            return getLocksForTx(tx).containsKey(kc);
        }
        
        public void take(StoreTransaction tx, KeyColumn kc, LockStatus ls) {
            getLocksForTx(tx).put(kc, ls);
        }
        
        public void release(StoreTransaction tx, KeyColumn kc) {
            getLocksForTx(tx).remove(kc);
        }
        
        public Map<KeyColumn, LockStatus> getLocksForTx(StoreTransaction tx) {
            Map<KeyColumn, LockStatus> m = locks.get(tx);
            
            if (null == m) {
                m = new HashMap<KeyColumn, LockStatus>();
                final Map<KeyColumn, LockStatus> x = locks.putIfAbsent(tx, m);
                if (null != x && x != m) {
                    m = x;
                }
            }
            
            return m;
        }
    }

}
