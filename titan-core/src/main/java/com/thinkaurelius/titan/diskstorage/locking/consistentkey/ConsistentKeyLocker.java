package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanConfigurationException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A global {@link Locker} that resolves inter-thread lock contention via
 * {@link AbstractLocker} and resolves inter-process contention by reading and
 * writing lock data using {@link KeyColumnValueStore}.
 * <p/>
 * <h2>Protocol and internals</h2>
 * <p/>
 * Locking is done in two stages: first between threads inside a shared process,
 * and then between processes in a Titan cluster.
 * <p/>
 * <h3>Inter-thread lock contention</h3>
 * <p/>
 * Lock contention between transactions within a shared process is arbitrated by
 * the {@code LocalLockMediator} class. This mediator uses standard
 * {@code java.util.concurrent} classes to guarantee that at most one thread
 * holds a lock on any given {@link KeyColumn} at any given time. The code that
 * uses a mediator to resolve inter-thread lock contention is common to multiple
 * {@code Locker} implementations and lives in the abstract base class
 * {@link AbstractLocker}.
 * <p/>
 * However, the mediator has no way to perform inter-process communication. The
 * mediator can't detect or prevent a thread in another process (potentially on
 * different machine) acquiring the same lock. This is addressed in the next
 * section.
 * <p/>
 * <h3>Inter-process lock contention</h3>
 * <p/>
 * After the mediator signals that the current transaction has obtained a lock
 * at the inter-thread/intra-process level, this implementation does the
 * following series of writes and reads to {@code KeyColumnValueStore} to check
 * whether it is the only process that holds the lock. These Cassandra
 * operations go to a dedicated store holding nothing but locking data (a
 * "store" in this context means a Cassandra column family, an HBase table,
 * etc.)
 * <p/>
 * <h4>Locking I/O sequence</h4>
 * <p/>
 * <ol>
 * <li>Write a single column to the store with the following data
 * <dl>
 * <dt>key</dt>
 * <dd>{@link KeyColumn#getKey()} followed by {@link KeyColumn#getColumn()}.</dd>
 * <dt>column</dt>
 * <dd>the approximate current timestamp in nanoseconds followed by this
 * process's {@code rid} (an opaque identifier which uniquely identifie
 * this process either globally or at least within the Titan cluster)</dd>
 * <dt>value</dt>
 * <dd>the single byte 0; this is unused but reserved for future use</dd>
 * </dl>
 * </li>
 * <p/>
 * <li>If the write failed or took longer than {@code lockWait} to complete
 * successfully, then retry the write with an updated timestamp and everything
 * else the same until we either exceed the configured retry count (in which
 * case we abort the lock attempt) or successfully complete the write in less
 * than {@code lockWait}.</li>
 * <p/>
 * <li>Wait, if necessary, until the time interval {@code lockWait} has passed
 * between the timestamp on our successful write and the current time.</li>
 * <p/>
 * <li>Read all columns for the key we wrote in the first step.</li>
 * <p/>
 * <li>Discard any columns with timestamps older than {@code lockExpire}.</li>
 * <p/>
 * <li>If our column is either the first column read or is preceeded only by
 * columns containing our own {@code rid}, then we hold the lock.  Otherwise,
 * another process holds the lock and we have failed to acquire it.</li>
 * <p/>
 * <li>To release the lock, we delete from the store the column that we
 * wrote earlier in this sequence</li>
 * </ol>
 * <p/>
 * <p/>
 * As mentioned earlier, this class relies on {@link AbstractLocker} to obtain
 * and release an intra-process lock before and after the sequence of steps
 * listed above.  The mediator step is necessary for thread-safety, because
 * {@code rid} is only unique at the process level.  Without a mediator, distinct
 * threads could write lock columns with the same {@code rid} and be unable to
 * tell their lock claims apart.
 */
public class ConsistentKeyLocker extends AbstractLocker<ConsistentKeyLockStatus> implements Locker {

    /**
     * Storage backend for locking records.
     */
    private final KeyColumnValueStore store;

    private final long lockWaitNS;

    private final int lockRetryCount;

    private static final StaticBuffer zeroBuf = ByteBufferUtil.getIntBuffer(0); // TODO this does not belong here

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyLocker.class);

    public static class Builder extends AbstractLocker.Builder<ConsistentKeyLockStatus, Builder> {
        // Required (no default)
        private final KeyColumnValueStore store;

        // Optional (has default)
        private long lockWaitNS;
        private int lockRetryCount;

        public Builder(KeyColumnValueStore store) {
            this.store = store;
            this.lockWaitNS = NANOSECONDS.convert(GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT, MILLISECONDS);
            this.lockRetryCount = GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT;
        }

        public Builder lockWaitNS(long wait, TimeUnit unit) {
            this.lockWaitNS = NANOSECONDS.convert(wait, unit);
            return self();
        }

        public Builder lockRetryCount(int count) {
            this.lockRetryCount = count;
            return self();
        }

        public Builder fromCommonsConfig(Configuration config) {
            rid(new StaticArrayBuffer(DistributedStoreManager.getRid(config)));

            final String llmPrefix = config.getString(
                    ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY);

            if (null != llmPrefix) {
                mediator(LocalLockMediators.INSTANCE.<StoreTransaction>get(llmPrefix));
            }

            lockRetryCount(config.getInt(
                    GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
                    GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT));

            lockWaitNS(config.getLong(
                    GraphDatabaseConfiguration.LOCK_WAIT_MS,
                    GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT), TimeUnit.MILLISECONDS);

            lockExpireNS(config.getLong(
                    GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
                    GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT), TimeUnit.MILLISECONDS);

            return this;
        }

        public ConsistentKeyLocker build() {
            preBuild();
            return new ConsistentKeyLocker(store, rid, times, serializer, llm, lockWaitNS, lockRetryCount, lockExpireNS, lockState);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected LocalLockMediator<StoreTransaction> getDefaultMediator() {
            throw new TitanConfigurationException("Local lock mediator prefix must not be empty or null");
        }
    }

    /**
     * Create a new locker.
     *
     * @param conf locker configuration
     */
    private ConsistentKeyLocker(KeyColumnValueStore store, StaticBuffer rid,
                                TimestampProvider times, ConsistentKeyLockerSerializer serializer,
                                LocalLockMediator<StoreTransaction> llm, long lockWaitNS,
                                int lockRetryCount, long lockExpireNS,
                                LockerState<ConsistentKeyLockStatus> lockState) {
        super(rid, times, serializer, llm, lockState, lockExpireNS, log);
        this.store = store;
        this.lockWaitNS = lockWaitNS;
        this.lockRetryCount = lockRetryCount;
    }

    private long getLockWait(TimeUnit tu) {
        return tu.convert(lockWaitNS, TimeUnit.NANOSECONDS);
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
     * @param lockID lock to acquire
     * @param txh    transaction
     * @return the timestamp, in nanoseconds since UNIX Epoch, on the lock
     *         column that we successfully wrote to the store
     * @throws TemporaryLockingException if the lock retry count is exceeded without successfully
     *                                   writing the lock in less than the wait limit
     * @throws Throwable                 if the storage layer throws anything else
     */
    @Override
    protected ConsistentKeyLockStatus writeSingleLock(KeyColumn lockID, StoreTransaction txh) throws Throwable {

        final StaticBuffer lockKey = serializer.toLockKey(lockID.getKey(), lockID.getColumn());
        StaticBuffer oldLockCol = null;

        for (int i = 0; i < lockRetryCount; i++) {
            WriteResult wr = tryWriteLockOnce(lockKey, oldLockCol, txh);
            if (wr.isSuccessful() && wr.getDurationNS() <= getLockWait(TimeUnit.NANOSECONDS)) {
//                log.debug("Wrote lock {} to store {} using {}", new Object[] { lockID, store.getName(), txh });
                return new ConsistentKeyLockStatus(
                        wr.getBeforeNS(), TimeUnit.NANOSECONDS,
                        wr.getBeforeNS() + lockExpireNS, TimeUnit.NANOSECONDS);
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
     * @param lockID  coordinates identifying the lock we tried but failed to
     *                acquire
     * @param lockKey the byte value of the key that we mutated or attempted to
     *                mutate in the lock store
     * @param wr      result of the mutation
     * @param txh     transaction attempting the lock
     * @throws Throwable if {@link WriteResult#getThrowable()} is not an instance of
     *                   {@link TemporaryStorageException}
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
                    getLockWait(TimeUnit.MILLISECONDS));
        }
    }

    private WriteResult tryWriteLockOnce(StaticBuffer key, StaticBuffer del, StoreTransaction txh) {
        Throwable t = null;
        final long before = times.getApproxNSSinceEpoch();
        StaticBuffer newLockCol = serializer.toLockCol(before, rid);
        Entry newLockEntry = new StaticBufferEntry(newLockCol, zeroBuf);
        try {
            store.mutate(key, Arrays.asList(newLockEntry), null == del ? ImmutableList.<StaticBuffer>of() : Arrays.asList(del), overrideTimestamp(txh, before));
        } catch (StorageException e) {
            t = e;
        }
        final long after = times.getApproxNSSinceEpoch();

        return new WriteResult(before, after, newLockCol, t);
    }

    private WriteResult tryDeleteLockOnce(StaticBuffer key, StaticBuffer col, StoreTransaction txh) {
        Throwable t = null;
        final long before = times.getApproxNSSinceEpoch();
        try {
            store.mutate(key, ImmutableList.<Entry>of(), Arrays.asList(col), overrideTimestamp(txh, before));
        } catch (StorageException e) {
            t = e;
        }
        final long after = times.getApproxNSSinceEpoch();
        return new WriteResult(before, after, null, t);
    }

    @Override
    protected void checkSingleLock(final KeyColumn kc, final ConsistentKeyLockStatus ls, final StoreTransaction tx) throws StorageException, InterruptedException {

        if (ls.isChecked())
            return;

        // Sleep, if necessary
        // We could be smarter about sleeping by iterating oldest -> latest...
        final long nowNS = times.sleepUntil(ls.getWriteTimestamp(TimeUnit.NANOSECONDS) + getLockWait(TimeUnit.NANOSECONDS));

        // Slice the store
        KeySliceQuery ksq = new KeySliceQuery(serializer.toLockKey(kc.getKey(), kc.getColumn()), ByteBufferUtil.zeroBuffer(9), ByteBufferUtil.oneBuffer(9));
        List<Entry> claimEntries = getSliceWithRetries(ksq, tx);

        // Extract timestamp and rid from the column in each returned Entry...
        Iterable<TimestampRid> iter = Iterables.transform(claimEntries, new Function<Entry, TimestampRid>() {
            @Override
            public TimestampRid apply(Entry e) {
                return serializer.fromLockColumn(e.getColumn());
            }
        });
        // ...and then filter out the TimestampRid objects with expired timestamps
        iter = Iterables.filter(iter, new Predicate<TimestampRid>() {
            @Override
            public boolean apply(TimestampRid tr) {
                if (tr.getTimestamp() < nowNS - lockExpireNS) {
                    log.warn("Discarded expired claim on {} with timestamp {}", kc, tr.getTimestamp());
                    return false;
                }
                return true;
            }
        });

        checkSeniority(kc, ls, iter);
        ls.setChecked();
    }

    private List<Entry> getSliceWithRetries(KeySliceQuery ksq, StoreTransaction tx) throws StorageException {

        for (int i = 0; i < lockRetryCount; i++) {
            // TODO either make this like writeLock so that it handles all Throwable types (and pull that logic out into a shared method) or make writeLock like this in that it only handles Temporary/PermanentSE
            try {
                return store.getSlice(ksq, tx);
            } catch (PermanentStorageException e) {
                log.error("Failed to check locks", e);
                throw new PermanentLockingException(e);
            } catch (TemporaryStorageException e) {
                log.warn("Temporary storage failure while checking locks", e);
            }
        }

        throw new TemporaryStorageException("Maximum retries (" + lockRetryCount + ") exceeded while checking locks");
    }

    private void checkSeniority(KeyColumn target, ConsistentKeyLockStatus ls, Iterable<TimestampRid> claimTRs) throws StorageException {

        int trCount = 0;

        for (TimestampRid tr : claimTRs) {
            trCount++;

            if (!rid.equals(tr.getRid())) {
                final String msg = "Lock on " + target + " already held by " + tr.getRid() + " (we are " + rid + ")";
                log.debug(msg);
                throw new TemporaryLockingException(msg);
            }

            if (tr.getTimestamp() == ls.getWriteTimestamp(TimeUnit.NANOSECONDS)) {
//                log.debug("Checked lock {} in store {}", target, store.getName());
                log.debug("Checked lock {}", target);
                return;
            }

            log.warn("Skipping outdated lock on {} with our rid ({}) but mismatched timestamp (actual ts {}, expected ts {})",
                    new Object[]{target, tr.getRid(), tr.getTimestamp(),
                            ls.getWriteTimestamp(TimeUnit.NANOSECONDS)});
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
            throw new TemporaryLockingException("No lock columns found for " + target);
        } else {
            final String msg = "Read "
                    + trCount
                    + " locks with our rid "
                    + rid
                    + " but mismatched timestamps; no lock column contained our timestamp ("
                    + ls.getWriteTimestamp(TimeUnit.NANOSECONDS) + ")";
            throw new PermanentStorageException(msg);
        }
    }

    @Override
    protected void deleteSingleLock(KeyColumn kc, ConsistentKeyLockStatus ls, StoreTransaction tx) {
        List<StaticBuffer> dels = ImmutableList.of(serializer.toLockCol(ls.getWriteTimestamp(TimeUnit.NANOSECONDS), rid));
        for (int i = 0; i < lockRetryCount; i++) {
            try {
                long before = times.getApproxNSSinceEpoch();
                store.mutate(serializer.toLockKey(kc.getKey(), kc.getColumn()), ImmutableList.<Entry>of(), dels, overrideTimestamp(tx, before));
                return;
            } catch (TemporaryStorageException e) {
                log.warn("Temporary storage exception while deleting lock", e);
                // don't return -- iterate and retry
            } catch (StorageException e) {
                log.error("Storage exception while deleting lock", e);
                return; // give up on this lock
            }
        }
    }
    
    private static StoreTransaction overrideTimestamp(final StoreTransaction tx, final long nanoTimestamp) {
        tx.getConfiguration().setTimestamp(nanoTimestamp);
        return tx;
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
}
