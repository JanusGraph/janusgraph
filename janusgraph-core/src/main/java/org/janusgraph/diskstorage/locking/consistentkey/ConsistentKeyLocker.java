// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.locking.consistentkey;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.AbstractLocker;
import org.janusgraph.diskstorage.locking.LocalLockMediator;
import org.janusgraph.diskstorage.locking.LocalLockMediators;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.LockerState;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.janusgraph.util.encoding.StringEncoding.UTF8_CHARSET;

/**
 * A global {@link Locker} that resolves inter-thread lock contention via
 * {@link AbstractLocker} and resolves inter-process contention by reading and
 * writing lock data using {@link KeyColumnValueStore}.
 * <p>
 * <h2>Protocol and internals</h2>
 * <p>
 * Locking is done in two stages: first between threads inside a shared process,
 * and then between processes in a JanusGraph cluster.
 * <p>
 * <h3>Inter-thread lock contention</h3>
 * <p>
 * Lock contention between transactions within a shared process is arbitrated by
 * the {@code LocalLockMediator} class. This mediator uses standard
 * {@code java.util.concurrent} classes to guarantee that at most one thread
 * holds a lock on any given {@link org.janusgraph.diskstorage.util.KeyColumn} at any given time. The code that
 * uses a mediator to resolve inter-thread lock contention is common to multiple
 * {@code Locker} implementations and lives in the abstract base class
 * {@link AbstractLocker}.
 * <p>
 * However, the mediator has no way to perform inter-process communication. The
 * mediator can't detect or prevent a thread in another process (potentially on
 * different machine) acquiring the same lock. This is addressed in the next
 * section.
 * <p>
 * <h3>Inter-process lock contention</h3>
 * <p>
 * After the mediator signals that the current transaction has obtained a lock
 * at the inter-thread/intra-process level, this implementation does the
 * following series of writes and reads to {@code KeyColumnValueStore} to check
 * whether it is the only process that holds the lock. These Cassandra
 * operations go to a dedicated store holding nothing but locking data (a
 * "store" in this context means a Cassandra column family, an HBase table,
 * etc.)
 * <p>
 * <h4>Locking I/O sequence</h4>
 * <p>
 * <ol>
 * <li>Write a single column to the store with the following data
 * <dl>
 * <dt>key</dt>
 * <dd>{@link org.janusgraph.diskstorage.util.KeyColumn#getKey()} followed by {@link org.janusgraph.diskstorage.util.KeyColumn#getColumn()}.</dd>
 * <dt>column</dt>
 * <dd>the approximate current timestamp in nanoseconds followed by this
 * process's {@code rid} (an opaque identifier which uniquely identify
 * this process either globally or at least within the JanusGraph cluster)</dd>
 * <dt>value</dt>
 * <dd>the single byte 0; this is unused but reserved for future use</dd>
 * </dl>
 * </li>
 * <li>If the write failed or took longer than {@code lockWait} to complete
 * successfully, then retry the write with an updated timestamp and everything
 * else the same until we either exceed the configured retry count (in which
 * case we abort the lock attempt) or successfully complete the write in less
 * than {@code lockWait}.</li>
 * <li>Wait, if necessary, until the time interval {@code lockWait} has passed
 * between the timestamp on our successful write and the current time.</li>
 * <li>Read all columns for the key we wrote in the first step.</li>
 * <li>Discard any columns with timestamps older than {@code lockExpire}.</li>
 * <li>If our column is either the first column read or is preceded only by
 * columns containing our own {@code rid}, then we hold the lock.  Otherwise,
 * another process holds the lock and we have failed to acquire it.</li>
 * <li>To release the lock, we delete from the store the column that we
 * wrote earlier in this sequence</li>
 * </ol>
 * <p>
 * <p>
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
    private final Duration lockWait;

    private final int lockRetryCount;

    /**
     * Expired lock cleaner in charge of {@link #store}.
     */
    private final LockCleanerService cleanerService;

    private static final StaticBuffer zeroBuf = BufferUtil.getIntBuffer(0); // TODO this does not belong here

    /*
     * In the storage backends, columns composed of one or more occurrences
     * of a single byte sort from shortest to longest.
     *
     * A lock column is 9 or more bytes long:
     *
     * -------------------------------------
     * | 8 bytes timestamp | var bytes rid |
     * -------------------------------------
     *
     * A start bound of a single zero byte will always sort before the
     * smallest timestamp due to length.
     *
     * The end bound is not as obvious.  A timestamp with all-one-bits
     * is eons away in the default configuration.  However, it's theoretically
     * possible with the nanos provider, since it relies on System.nanoTime,
     * and the contract for nanoTime explicitly says that it may go negative.
     *
     * Fortunately, we can rely on the rid for our end bound.  The rid is a
     * user-customizable string that gets converted into a StaticBuffer via
     * String.getBytes.  This should UTF-8 encode it.  Under UTF-8, a byte
     * with all bits set is illegal.  So, the 9th byte of a lock column
     * should never have all bits set.  This is why LOCK_END_COL is exactly
     * 9 bytes long: it's just enough to extend past the timestamp and onto
     * the first byte of a UTF-8 string, and that UTF-8 byte should have at
     * least one zero bit that makes it sort before.
     */
    public static final StaticBuffer LOCK_COL_START = BufferUtil.zeroBuffer(1);
    public static final StaticBuffer LOCK_COL_END   = BufferUtil.oneBuffer(9);

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyLocker.class);

    public static class Builder extends AbstractLocker.Builder<ConsistentKeyLockStatus, Builder> {
        // Required (no default)
        private final KeyColumnValueStore store;
        private final StoreManager manager;

        // Optional (has default)
        private Duration lockWait;
        private int lockRetryCount;

        private enum CleanerConfig {
            NONE,
            STANDARD,
            CUSTOM
        }

        private CleanerConfig cleanerConfig = CleanerConfig.NONE;
        private LockCleanerService customCleanerService;

        public Builder(KeyColumnValueStore store, StoreManager manager) {
            this.store = store;
            this.manager = manager;
            this.lockWait = GraphDatabaseConfiguration.LOCK_WAIT.getDefaultValue();
            this.lockRetryCount = GraphDatabaseConfiguration.LOCK_RETRY.getDefaultValue();
        }

        public Builder lockWait(Duration d) {
            this.lockWait = d;
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
            rid(new StaticArrayBuffer(config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID)
                .getBytes(UTF8_CHARSET)));

            final String llmPrefix = config.get(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP);

            times(config.get(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER));

            mediator(LocalLockMediators.INSTANCE.get(llmPrefix, times));

            lockRetryCount(config.get(GraphDatabaseConfiguration.LOCK_RETRY));

            lockWait(config.get(GraphDatabaseConfiguration.LOCK_WAIT));

            lockExpire(config.get(GraphDatabaseConfiguration.LOCK_EXPIRE));

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
                cleaner = new StandardLockCleanerService(store, serializer, times);
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
                    lockWait,
                    lockRetryCount,
                    lockExpire,
                    lockState, cleaner);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected LocalLockMediator<StoreTransaction> getDefaultMediator() {
            throw new JanusGraphConfigurationException("Local lock mediator prefix must not be empty or null");
        }
    }

    /**
     * Create a new locker.
     *
     */
    private ConsistentKeyLocker(KeyColumnValueStore store, StoreManager manager, StaticBuffer rid,
                                TimestampProvider times, ConsistentKeyLockerSerializer serializer,
                                LocalLockMediator<StoreTransaction> llm, Duration lockWait,
                                int lockRetryCount, Duration lockExpire,
                                LockerState<ConsistentKeyLockStatus> lockState,
                                LockCleanerService cleanerService) {
        super(rid, times, serializer, llm, lockState, lockExpire, log);
        this.store = store;
        this.manager = manager;
        this.lockWait = lockWait;
        this.lockRetryCount = lockRetryCount;
        this.cleanerService = cleanerService;
    }

    /**
     * Try to write a lock record remotely up to the configured number of
     *  times. If the store produces
     * {@link TemporaryLockingException}, then we'll call mutate again to add a
     * new column with an updated timestamp and to delete the column that tried
     * to write when the store threw an exception. We continue like that up to
     * the retry limit. If the store throws anything else, such as an unchecked
     * exception or a {@link org.janusgraph.diskstorage.PermanentBackendException}, then we'll try to
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
            if (wr.isSuccessful() && wr.getDuration().compareTo(lockWait) <= 0) {
                final Instant writeInstant = wr.getWriteTimestamp();
                final Instant expireInstant = writeInstant.plus(lockExpire);
                return new ConsistentKeyLockStatus(writeInstant, expireInstant);
            }
            oldLockCol = wr.getLockCol();
            handleMutationFailure(lockID, lockKey, wr, txh);
        }
        tryDeleteLockOnce(lockKey, oldLockCol, txh);
        // TODO log exception or successful too-slow write here
        throw new TemporaryBackendException("Lock write retry count exceeded");
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
     *                   {@link org.janusgraph.diskstorage.TemporaryBackendException}
     */
    private void handleMutationFailure(KeyColumn lockID, StaticBuffer lockKey, WriteResult wr,
                                       StoreTransaction txh) throws Throwable {
        Throwable error = wr.getThrowable();
        if (null != error) {
            if (error instanceof TemporaryBackendException) {
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
                    log.warn("Failed to delete lock write: abandoning potentially-unreleased lock on {}",
                        lockID, dwr.getThrowable());
                }
                throw error;
            }
        } else {
            log.warn("Lock write succeeded but took too long: duration {} exceeded limit {}",
                wr.getDuration(), lockWait);
        }
    }

    private WriteResult tryWriteLockOnce(StaticBuffer key, StaticBuffer del, StoreTransaction txh) {
        Throwable t = null;
        final Timer writeTimer = times.getTimer().start();
        StaticBuffer newLockCol = serializer.toLockCol(writeTimer.getStartTime(), rid, times);
        Entry newLockEntry = StaticArrayEntry.of(newLockCol, zeroBuf);
        StoreTransaction newTx = null;
        try {
            newTx = overrideTimestamp(txh, writeTimer.getStartTime());

            store.mutate(key, Collections.singletonList(newLockEntry),
                null == del ? KeyColumnValueStore.NO_DELETIONS : Collections.singletonList(del), newTx);

            newTx.commit();
            newTx = null;
        } catch (BackendException e) {
            log.debug("Lock write attempt failed with exception", e);
            t = e;
        } finally {
            rollbackIfNotNull(newTx);
        }
        writeTimer.stop();

        return new WriteResult(writeTimer.elapsed(), writeTimer.getStartTime(), newLockCol, t);
    }

    private WriteResult tryDeleteLockOnce(StaticBuffer key, StaticBuffer col, StoreTransaction txh) {
        Throwable t = null;
        final Timer delTimer = times.getTimer().start();
        StoreTransaction newTx = null;
        try {
            newTx = overrideTimestamp(txh, delTimer.getStartTime());

            store.mutate(key, Collections.emptyList(), Collections.singletonList(col), newTx);

            newTx.commit();
            newTx = null;
        } catch (BackendException e) {
            t = e;
        } finally {
            rollbackIfNotNull(newTx);
        }
        delTimer.stop();

        return new WriteResult(delTimer.elapsed(), delTimer.getStartTime(), null, t);
    }

    @Override
    protected void checkSingleLock(final KeyColumn kc, final ConsistentKeyLockStatus ls,
                                   final StoreTransaction tx) throws BackendException, InterruptedException {

        if (ls.isChecked())
            return;

        // Sleep, if necessary
        // We could be smarter about sleeping by iterating oldest -> latest...
        final Instant now = times.sleepPast(ls.getWriteTimestamp().plus(lockWait));

        // Slice the store
        KeySliceQuery ksq = new KeySliceQuery(serializer.toLockKey(kc.getKey(), kc.getColumn()), LOCK_COL_START,
            LOCK_COL_END);
        List<Entry> claimEntries = getSliceWithRetries(ksq, tx);

        // Extract timestamp and rid from the column in each returned Entry...
        final Iterable<TimestampRid> iterable = Iterables.transform(claimEntries,
            e -> serializer.fromLockColumn(e.getColumnAs(StaticBuffer.STATIC_FACTORY), times));
        // ...and then filter out the TimestampRid objects with expired timestamps
        // (This doesn't use Iterables.filter and Predicate so that we can throw a checked exception if necessary)
        final List<TimestampRid> unexpiredTRs = new ArrayList<>(Iterables.size(iterable));
        final Instant cutoffTime = now.minus(lockExpire);
        for (TimestampRid tr : iterable) {
            if (tr.getTimestamp().isBefore(cutoffTime)) {
                log.warn("Discarded expired claim on {} with timestamp {}", kc, tr.getTimestamp());
                if (null != cleanerService)
                    cleanerService.clean(kc, cutoffTime, tx);
                // Locks that this instance wrote that have now expired should not only log
                // but also throw a descriptive exception
                if (rid.equals(tr.getRid()) && ls.getWriteTimestamp().equals(tr.getTimestamp())) {
                    throw new ExpiredLockException("Expired lock on " + kc +
                            ": lock timestamp " + tr.getTimestamp() + " " + times.getUnit() + " is older than " +
                            ConfigElement.getPath(GraphDatabaseConfiguration.LOCK_EXPIRE) + "=" + lockExpire);
                    // Really shouldn't refer to GDC.LOCK_EXPIRE here,
                    // but this will typically be accurate in a real use case
                }
                continue;
            }
            unexpiredTRs.add(tr);
        }

        checkSeniority(kc, ls, unexpiredTRs);
        ls.setChecked();
    }

    private List<Entry> getSliceWithRetries(KeySliceQuery ksq, StoreTransaction tx) throws BackendException {

        for (int i = 0; i < lockRetryCount; i++) {
            // TODO either make this like writeLock so that it handles all Throwable types (and pull that logic out
            // into a shared method) or make writeLock like this in that it only handles Temporary/PermanentSE
            try {
                return store.getSlice(ksq, tx);
            } catch (PermanentBackendException e) {
                log.error("Failed to check locks", e);
                throw new PermanentLockingException(e);
            } catch (TemporaryBackendException e) {
                log.warn("Temporary storage failure while checking locks", e);
            }
        }

        throw new TemporaryBackendException("Maximum retries (" + lockRetryCount + ") exceeded while checking locks");
    }

    private void checkSeniority(KeyColumn target, ConsistentKeyLockStatus ls,
                                Iterable<TimestampRid> claimTRs) throws BackendException {

        int trCount = 0;

        for (TimestampRid tr : claimTRs) {
            trCount++;

            if (!rid.equals(tr.getRid())) {
                final String msg = "Lock on " + target + " already held by " + tr.getRid() + " (we are " + rid + ")";
                log.debug(msg);
                throw new TemporaryLockingException(msg);
            }

            if (tr.getTimestamp().equals(ls.getWriteTimestamp())) {
//                log.debug("Checked lock {} in store {}", target, store.getName());
                log.debug("Checked lock {}", target);
                return;
            }

            log.warn("Skipping outdated lock on {} with our rid ({}) but mismatched timestamp (actual ts {}, expected ts {})",
                target, tr.getRid(), tr.getTimestamp(), ls.getWriteTimestamp());
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
                    + ls.getWriteTimestamp() + ")";
            throw new PermanentBackendException(msg);
        }
    }

    @Override
    protected void deleteSingleLock(KeyColumn kc, ConsistentKeyLockStatus ls, StoreTransaction tx) {
        List<StaticBuffer> deletions = Collections.singletonList(serializer.toLockCol(ls.getWriteTimestamp(), rid, times));
        for (int i = 0; i < lockRetryCount; i++) {
            StoreTransaction newTx = null;
            try {
                newTx = overrideTimestamp(tx, times.getTime());
                store.mutate(serializer.toLockKey(kc.getKey(), kc.getColumn()), Collections.emptyList(), deletions, newTx);

                newTx.commit();
                newTx = null;
                return;
            } catch (TemporaryBackendException e) {
                log.warn("Temporary storage exception while deleting lock", e);
                // don't return -- iterate and retry
            } catch (BackendException e) {
                log.error("Storage exception while deleting lock", e);
                return; // give up on this lock
            } finally {
                rollbackIfNotNull(newTx);
            }
        }
    }

    private StoreTransaction overrideTimestamp(final StoreTransaction tx,
                                               final Instant commitTime) throws BackendException {
        StandardBaseTransactionConfig newCfg = new StandardBaseTransactionConfig.Builder(tx.getConfiguration())
               .commitTime(commitTime).build();
        return manager.beginTransaction(newCfg);
    }

    private void rollbackIfNotNull(StoreTransaction tx) {
        if (tx != null) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Transaction is still open! Rolling back: " + tx, new Throwable());
                }

                tx.rollback();
            } catch (Throwable excp) {
                log.error("Failed to rollback transaction " + tx + ". The transaction may be leaked.", excp);
            }
        }

    }

    private static class WriteResult {
        private final Duration duration;
        private final Instant writeTimestamp;
        private final StaticBuffer lockCol;
        private final Throwable throwable;

        public WriteResult(Duration duration, Instant writeTimestamp, StaticBuffer lockCol, Throwable throwable) {
            this.duration = duration;
            this.writeTimestamp = writeTimestamp;
            this.lockCol = lockCol;
            this.throwable = throwable;
        }

        public Duration getDuration() {
            return duration;
        }

        public Instant getWriteTimestamp() {
            return writeTimestamp;
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
