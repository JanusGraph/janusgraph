package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanConfigurationException;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.*;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    private final StoreManager manager;

    /**
     * This has units of {@code times.getUnit()}.
     */
    private final long lockWait;

    private final int lockRetryCount;

    /**
     * Expired lock cleaner in charge of {@link #store}.
     */
    private final LockCleanerService cleanerService;

    private static final StaticBuffer zeroBuf = BufferUtil.getIntBuffer(0); // TODO this does not belong here

    public static final StaticBuffer LOCK_COL_START = BufferUtil.zeroBuffer(9);
    public static final StaticBuffer LOCK_COL_END   = BufferUtil.oneBuffer(9);

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyLocker.class);

    public static class Builder extends AbstractLocker.Builder<ConsistentKeyLockStatus, Builder> {
        // Required (no default)
        private final KeyColumnValueStore store;
        private final StoreManager manager;

        // Optional (has default)
        private long lockWait;
        private TimeUnit lockWaitUnit;
        private int lockRetryCount;

        private enum CleanerConfig {
            NONE,
            STANDARD,
            CUSTOM
        };

        private CleanerConfig cleanerConfig = CleanerConfig.NONE;
        private LockCleanerService customCleanerService;

        public Builder(KeyColumnValueStore store, StoreManager manager) {
            this.store = store;
            this.manager = manager;
            this.lockWait = GraphDatabaseConfiguration.LOCK_WAIT.getDefaultValue();
            this.lockWaitUnit = TimeUnit.MILLISECONDS;
            this.lockRetryCount = GraphDatabaseConfiguration.LOCK_RETRY.getDefaultValue();
        }

        public Builder lockWait(long wait, TimeUnit unit) {
            this.lockWait = wait;
            this.lockWaitUnit = unit;
            return self();
        }

        public Builder lockRetryCount(int count) {
            this.lockRetryCount = count;
            return self();
        }

        public Builder standardCleaner() {
            this.cleanerConfig = CleanerConfig.STANDARD;
            this.customCleanerService = null;
            return self();
        }

        public Builder customCleaner(LockCleanerService s) {
            this.cleanerConfig = CleanerConfig.CUSTOM;
            this.customCleanerService = s;
            Preconditions.checkNotNull(this.customCleanerService);
            return self();
        }

        public Builder fromConfig(Configuration config) {
            rid(new StaticArrayBuffer(DistributedStoreManager.getRid(config)));

            final String llmPrefix = config.get(
                    ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX);
            mediator(LocalLockMediators.INSTANCE.<StoreTransaction>get(llmPrefix));

            times(config.get(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER));

            lockRetryCount(config.get(GraphDatabaseConfiguration.LOCK_RETRY));

            lockWait(config.get(GraphDatabaseConfiguration.LOCK_WAIT), TimeUnit.MILLISECONDS);

            lockExpire(config.get(GraphDatabaseConfiguration.LOCK_EXPIRE), TimeUnit.MILLISECONDS);

            if (config.get(GraphDatabaseConfiguration.LOCK_CLEAN_EXPIRED)) {
                standardCleaner();
            }

            return this;
        }

        public ConsistentKeyLocker build() {
            preBuild();

            final LockCleanerService cleaner;

            switch (cleanerConfig) {
            case STANDARD:
                Preconditions.checkArgument(null == customCleanerService);
                cleaner = new StandardLockCleanerService(store, serializer);
                break;
            case CUSTOM:
                Preconditions.checkArgument(null != customCleanerService);
                cleaner = customCleanerService;
                break;
            default:
                cleaner = null;
            }

            return new ConsistentKeyLocker(store, manager, rid, times,
                    serializer, llm,
                    times.getUnit().convert(lockWait, lockWaitUnit),
                    lockRetryCount,
                    times.getUnit().convert(lockExpire, lockExpireUnit),
                    lockState, cleaner);
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
     */
    private ConsistentKeyLocker(KeyColumnValueStore store, StoreManager manager, StaticBuffer rid,
                                TimestampProvider times, ConsistentKeyLockerSerializer serializer,
                                LocalLockMediator<StoreTransaction> llm, long lockWait,
                                int lockRetryCount, long lockExpire,
                                LockerState<ConsistentKeyLockStatus> lockState,
                                LockCleanerService cleanerService) {
        super(rid, times, serializer, llm, lockState, lockExpire, log);
        this.store = store;
        this.manager = manager;
        this.lockWait = lockWait;
        this.lockRetryCount = lockRetryCount;
        this.cleanerService = cleanerService;
    }

    private long getLockWait(TimeUnit tu) {
        return tu.convert(lockWait, timeUnit);
    }

    /**
     * Try to write a lock record remotely up to the configured number of
     *  times. If the store produces
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
            if (wr.isSuccessful() && wr.getDuration(timeUnit) <= getLockWait(timeUnit)) {
//                log.debug("Wrote lock {} to store {} using {}", new Object[] { lockID, store.getName(), txh });
                return new ConsistentKeyLockStatus(
                        wr.getBefore(timeUnit), timeUnit,
                        wr.getBefore(timeUnit) + lockExpire, timeUnit);
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
     * but took longer to complete than configured lock wait time, or that
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
        final long before = times.getTime();
        StaticBuffer newLockCol = serializer.toLockCol(before, rid);
        Entry newLockEntry = StaticArrayEntry.of(newLockCol, zeroBuf);
        try {
            StoreTransaction newTx = overrideTimestamp(txh, before);
            store.mutate(key, Arrays.asList(newLockEntry), null == del ? KeyColumnValueStore.NO_DELETIONS : Arrays.asList(del), newTx);
        } catch (StorageException e) {
            log.debug("Lock write attempt failed with exception", e);
            t = e;
        }
        final long after = times.getTime();

        return new WriteResult(before, after, timeUnit, newLockCol, t);
    }

    private WriteResult tryDeleteLockOnce(StaticBuffer key, StaticBuffer col, StoreTransaction txh) {
        Throwable t = null;
        final long before = times.getTime();
        try {
            StoreTransaction newTx = overrideTimestamp(txh, before);
            store.mutate(key, ImmutableList.<Entry>of(), Arrays.asList(col), newTx);
        } catch (StorageException e) {
            t = e;
        }
        final long after = times.getTime();
        return new WriteResult(before, after, timeUnit, null, t);
    }

    @Override
    protected void checkSingleLock(final KeyColumn kc, final ConsistentKeyLockStatus ls, final StoreTransaction tx) throws StorageException, InterruptedException {

        if (ls.isChecked())
            return;

        // Sleep, if necessary
        // We could be smarter about sleeping by iterating oldest -> latest...
        final long now = times.sleepPast(ls.getWriteTimestamp(timeUnit) + getLockWait(timeUnit), timeUnit);

        // Slice the store
        KeySliceQuery ksq = new KeySliceQuery(serializer.toLockKey(kc.getKey(), kc.getColumn()), LOCK_COL_START, LOCK_COL_END);
        List<Entry> claimEntries = getSliceWithRetries(ksq, tx);

        // Extract timestamp and rid from the column in each returned Entry...
        Iterable<TimestampRid> iter = Iterables.transform(claimEntries, new Function<Entry, TimestampRid>() {
            @Override
            public TimestampRid apply(Entry e) {
                return serializer.fromLockColumn(e.getColumnAs(StaticBuffer.STATIC_FACTORY));
            }
        });
        // ...and then filter out the TimestampRid objects with expired timestamps
        iter = Iterables.filter(iter, new Predicate<TimestampRid>() {
            @Override
            public boolean apply(TimestampRid tr) {
                final long cutoff = now - lockExpire;
                if (tr.getTimestamp() < cutoff) {
                    log.warn("Discarded expired claim on {} with timestamp {}", kc, tr.getTimestamp());
                    if (null != cleanerService)
                        cleanerService.clean(kc, cutoff, tx);
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

            if (tr.getTimestamp() == ls.getWriteTimestamp(timeUnit)) {
//                log.debug("Checked lock {} in store {}", target, store.getName());
                log.debug("Checked lock {}", target);
                return;
            }

            log.warn("Skipping outdated lock on {} with our rid ({}) but mismatched timestamp (actual ts {}, expected ts {})",
                    new Object[]{target, tr.getRid(), tr.getTimestamp(),
                            ls.getWriteTimestamp(timeUnit)});
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
                    + ls.getWriteTimestamp(timeUnit) + ")";
            throw new PermanentStorageException(msg);
        }
    }

    @Override
    protected void deleteSingleLock(KeyColumn kc, ConsistentKeyLockStatus ls, StoreTransaction tx) {
        List<StaticBuffer> dels = ImmutableList.of(serializer.toLockCol(ls.getWriteTimestamp(timeUnit), rid));
        for (int i = 0; i < lockRetryCount; i++) {
            try {
                StoreTransaction newTx = overrideTimestamp(tx, times.getTime());
                store.mutate(serializer.toLockKey(kc.getKey(), kc.getColumn()), ImmutableList.<Entry>of(), dels, newTx);
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

    private StoreTransaction overrideTimestamp(final StoreTransaction tx, final long ts) throws StorageException {
        StandardTransactionConfig newCfg = new StandardTransactionConfig.Builder(tx.getConfiguration(), ts).timestampProvider(times).build();
        return manager.beginTransaction(newCfg);
    }

    private static class WriteResult {
        private final long before;
        private final long after;
        private final TimeUnit timeUnit;
        private final StaticBuffer lockCol;
        private final Throwable throwable;

        public WriteResult(long before, long after, TimeUnit timeUnit, StaticBuffer lockCol, Throwable throwable) {
            this.before = before;
            this.after = after;
            this.timeUnit = timeUnit;
            this.lockCol = lockCol;
            this.throwable = throwable;
        }

        public long getBefore(TimeUnit tu) {
            return tu.convert(before, timeUnit);
        }

        public long getDuration(TimeUnit tu) {
            return tu.convert(after - before, timeUnit);
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
