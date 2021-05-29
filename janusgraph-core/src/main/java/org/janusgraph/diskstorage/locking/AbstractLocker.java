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

package org.janusgraph.diskstorage.locking;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLockStatus;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.stats.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

/**
 * Abstract base class for building lockers. Implements locking between threads
 * using {@link LocalLockMediator} but delegates inter-process lock resolution
 * to its subclasses.
 *
 * @param <S> An implementation-specific type holding information about a single
 *            lock; see {@link ConsistentKeyLockStatus} for an example
 * @see ConsistentKeyLocker
 */
public abstract class AbstractLocker<S extends LockStatus> implements Locker {

    /**
     * Uniquely identifies a process within a domain (or across all domains,
     * though only intra-domain uniqueness is required)
     */
    protected final StaticBuffer rid;

    /**
     * Sole source of time. All fields of type long that represent times or
     * durations should be expressed in terms of
     * {@link TimestampProvider#getUnit()}. Furthermore, if the locking backend
     * allows the client to set a timestamp on writes, those timestamps should
     * be in the same units.
     * <p>
     * Don't call {@link System#currentTimeMillis()} or
     * {@link System#nanoTime()} directly. Use only this object. This object is
     * replaced with a mock during testing to give tests exact control over the
     * flow of time.
     */
    protected final TimestampProvider times;

    /**
     * This is sort-of Cassandra/HBase specific. It concatenates
     * {@link KeyColumn} arguments into a single StaticBuffer containing the key
     * followed by the column and vice-versa.
     */
    protected final ConsistentKeyLockerSerializer serializer;

    /**
     * Resolves lock contention by multiple threads.
     */
    protected final LocalLockMediator<StoreTransaction> llm;

    /**
     * Stores all information about all locks this implementation has taken on
     * behalf of any {@link StoreTransaction}. It is parameterized in a type
     * specific to the concrete subclass, so that concrete implementations can
     * store information specific to their locking primitives.
     */
    protected final LockerState<S> lockState;

    /**
     * The amount of time, in {@link #times}{@code .getUnit()}, that may pass
     * after writing a lock before it is considered to be invalid and
     * automatically unlocked.
     */
    protected final Duration lockExpire;

    protected final Logger log;

    private static final String M_LOCKS = "locks";
    private static final String M_WRITE = "write";
    private static final String M_CHECK = "check";
    private static final String M_DELETE = "delete";
    private static final String M_CALLS = "calls";
    private static final String M_EXCEPTIONS = "exceptions";

    /**
     * Abstract builder for this Locker implementation. See
     * {@link ConsistentKeyLocker} for an example of how to subclass this
     * abstract builder into a concrete builder.
     * <p>
     * If you're wondering why the bounds for the type parameter {@code B} looks so hideous, see:
     * <p>
     * <a href="https://weblogs.java.net/blog/emcmanus/archive/2010/10/25/using-builder-pattern-subclasses">Using the builder pattern with subclasses by Eamonn McManus</a>
     *
     * @param <S> The concrete type of {@link LockStatus}
     * @param <B> The concrete type of the subclass extending this builder
     */
    public abstract static class Builder<S, B extends Builder<S, B>> {

        protected StaticBuffer rid;
        protected TimestampProvider times;
        protected ConsistentKeyLockerSerializer serializer;
        protected LocalLockMediator<StoreTransaction> llm;
        protected LockerState<S> lockState;
        protected Duration lockExpire;
        protected Logger log;

        public Builder() {
            this.rid = null; //TODO: can we ensure that this is always set correctly? Check the AstyanaxRecipe
            this.times = TimestampProviders.NANO;
            this.serializer = new ConsistentKeyLockerSerializer();
            this.llm = null; // redundant, but it preserves this constructor's overall pattern
            this.lockState = new LockerState<>();
            this.lockExpire = GraphDatabaseConfiguration.LOCK_EXPIRE.getDefaultValue();
            this.log = LoggerFactory.getLogger(AbstractLocker.class);
        }

        /**
         * Concrete subclasses should just "{@code return this;}".
         *
         * @return concrete subclass instance
         */
        protected abstract B self();

        public B rid(StaticBuffer rid) {
            this.rid = rid;
            return self();
        }

        public B times(TimestampProvider times) {
            this.times = times;
            return self();
        }

        public B serializer(ConsistentKeyLockerSerializer serializer) {
            this.serializer = serializer;
            return self();
        }

        public B mediator(LocalLockMediator<StoreTransaction> mediator) {
            this.llm = mediator;
            return self();
        }

        /**
         * Retrieve the mediator associated with {@code name} via {@link LocalLockMediators#get(String, TimestampProvider)}.
         *
         * @param name the mediator name
         * @return this builder
         */
        public B mediatorName(String name) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(times, "Timestamp provider must be set before initializing local lock mediator");
            mediator(LocalLockMediators.INSTANCE.get(name, times));
            return self();
        }

        public B logger(Logger log) {
            this.log = log;
            return self();
        }

        public B lockExpire(Duration d) {
            this.lockExpire = d;
            return self();
        }

        /**
         * This method is only intended for testing. Calling this in production
         * could cause lock failures.
         *
         * @param state the initial lock state for this instance
         * @return this builder
         */
        public B internalState(LockerState<S> state) {
            this.lockState = state;
            return self();
        }

        /**
         * Inspect and modify this builder's state after the client has called
         * {@code build()}, but before a return object has been instantiated.
         * This is useful for catching illegal values or translating placeholder
         * configuration values into the objects they represent. This is
         * intended to be called from subclasses' build() methods.
         */
        protected void preBuild() {
            if (null == llm) {
                llm = getDefaultMediator();
            }
        }

        /**
         * Get the default {@link LocalLockMediator} for Locker being built.
         * This is called when the client doesn't specify a locker.
         *
         * @return a lock mediator
         */
        protected abstract LocalLockMediator<StoreTransaction> getDefaultMediator();
    }

    public AbstractLocker(StaticBuffer rid, TimestampProvider times,
            ConsistentKeyLockerSerializer serializer,
            LocalLockMediator<StoreTransaction> llm, LockerState<S> lockState,
            Duration lockExpire, Logger log) {
        this.rid = rid;
        this.times = times;
        this.serializer = serializer;
        this.llm = llm;
        this.lockState = lockState;
        this.lockExpire = lockExpire;
        this.log = log;
    }

    /**
     * Try to take/acquire/write/claim a lock uniquely identified within this
     * {@code Locker} by the {@code lockID} argument on behalf of {@code tx}.
     *
     * @param lockID identifies the lock
     * @param tx     identifies the process claiming this lock
     * @return a {@code LockStatus} implementation on successful lock acquisition
     * @throws Throwable if the lock could not be taken/acquired/written/claimed or
     *                   the attempted write encountered an error
     */
    protected abstract S writeSingleLock(KeyColumn lockID, StoreTransaction tx) throws Throwable;

    /**
     * Try to verify that the lock identified by {@code lockID} is already held
     * by {@code tx}. The {@code lockStatus} argument refers to the object
     * returned by a previous call to
     * {@link #writeSingleLock(KeyColumn, StoreTransaction)}. This should be a
     * read-only operation: return if the lock is already held, but this method
     * finds that it is not held, then throw an exception instead of trying to
     * acquire it.
     * <p>
     * This method is only useful with nonblocking locking implementations try
     * to lock and then check the outcome of the attempt in two separate stages.
     * For implementations that build {@code writeSingleLock(...)} on a
     * synchronous locking primitive, such as a blocking {@code lock()} method
     * or a blocking semaphore {@code p()}, this method is redundant with
     * {@code writeSingleLock(...)} and may unconditionally return true.
     *
     * @param lockID     identifies the lock to check
     * @param lockStatus the result of a prior successful {@code writeSingleLock(...)}
     *                   call on this {@code lockID} and {@code tx}
     * @param tx         identifies the process claiming this lock
     * @throws Throwable if the lock fails the check or if the attempted check
     *                   encountered an error
     */
    protected abstract void checkSingleLock(KeyColumn lockID, S lockStatus, StoreTransaction tx) throws Throwable;

    /**
     * Try to unlock/release/delete the lock identified by {@code lockID} and
     * both held by and verified for {@code tx}. This method is only called with
     * arguments for which {@link #writeSingleLock(KeyColumn, StoreTransaction)}
     * and {@link #checkSingleLock(KeyColumn, LockStatus, StoreTransaction)}
     * both returned successfully (i.e. without exceptions).
     *
     * @param lockID     identifies the lock to release
     * @param lockStatus the result of a prior successful {@code writeSingleLock(...)}
     *                   followed by a successful {@code checkSingleLock(...)}
     * @param tx         identifies the process that wrote and checked this lock
     * @throws Throwable if the lock could not be released/deleted or if the attempted
     *                   delete encountered an error
     */
    protected abstract void deleteSingleLock(KeyColumn lockID, S lockStatus, StoreTransaction tx) throws Throwable;

    @Override
    public void writeLock(KeyColumn lockID, StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {

        if (null != tx.getConfiguration().getGroupName()) {
            MetricManager.INSTANCE.getCounter(tx.getConfiguration().getGroupName(), M_LOCKS, M_WRITE, M_CALLS).inc();
        }

        if (lockState.has(tx, lockID)) {
            log.debug("Transaction {} already wrote lock on {}", tx, lockID);
            return;
        }

        if (lockLocally(lockID, tx)) {
            boolean ok = false;
            try {
                S stat = writeSingleLock(lockID, tx);
                lockLocally(lockID, stat.getExpirationTimestamp(), tx); // update local lock expiration time
                lockState.take(tx, lockID, stat);
                ok = true;
            } catch (TemporaryBackendException tse) {
                throw new TemporaryLockingException(tse);
            } catch (AssertionError ae) {
                // Concession to ease testing with mocks & behavior verification
                ok = true;
                throw ae;
            } catch (Throwable t) {
                throw new PermanentLockingException(t);
            } finally {
                if (!ok) {
                    // lockState.release(tx, lockID); // has no effect
                    unlockLocally(lockID, tx);
                    if (null != tx.getConfiguration().getGroupName()) {
                        MetricManager.INSTANCE.getCounter(tx.getConfiguration().getGroupName(), M_LOCKS, M_WRITE, M_EXCEPTIONS).inc();
                    }
                }
            }
        } else {
            // Fail immediately with no retries on local contention
            throw new PermanentLockingException("Local lock contention");
        }
    }

    @Override
    public void checkLocks(StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {

        if (null != tx.getConfiguration().getGroupName()) {
            MetricManager.INSTANCE.getCounter(tx.getConfiguration().getGroupName(), M_LOCKS, M_CHECK, M_CALLS).inc();
        }

        Map<KeyColumn, S> m = lockState.getLocksForTx(tx);

        if (m.isEmpty()) {
            return; // no locks for this tx
        }

        // We never receive interrupts in normal operation; one can only appear
        // during Thread.sleep(), and in that case it probably means the entire
        // JanusGraph process is shutting down; for this reason, we return ASAP on an
        // interrupt
        boolean ok = false;
        try {
            for (final Map.Entry<KeyColumn, S> entry : m.entrySet()) {
                checkSingleLock(entry.getKey(), entry.getValue(), tx);
            }
            ok = true;
        } catch (TemporaryLockingException | PermanentLockingException | AssertionError tle) {
            throw tle;
        } catch (InterruptedException | TemporaryBackendException e) {
            throw new TemporaryLockingException(e);
        } catch (Throwable t) {
            throw new PermanentLockingException(t);
        } finally {
            if (!ok && null != tx.getConfiguration().getGroupName()) {
                MetricManager.INSTANCE.getCounter(tx.getConfiguration().getGroupName(), M_LOCKS, M_CHECK, M_CALLS).inc();
            }
        }
    }

    @Override
    public void deleteLocks(StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {
        if (null != tx.getConfiguration().getGroupName()) {
            MetricManager.INSTANCE.getCounter(tx.getConfiguration().getGroupName(), M_LOCKS, M_DELETE, M_CALLS).inc();
        }

        Map<KeyColumn, S> m = lockState.getLocksForTx(tx);

        final Iterator<Map.Entry<KeyColumn, S>> iterator = m.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<KeyColumn, S> entry = iterator.next();
            final KeyColumn kc = entry.getKey();
            final S ls = entry.getValue();
            try {
                deleteSingleLock(kc, ls, tx);
            } catch (AssertionError ae) {
                throw ae; // Concession to ease testing with mocks & behavior verification
            } catch (Throwable t) {
                log.error("Exception while deleting lock on " + kc, t);
                if (null != tx.getConfiguration().getGroupName()) {
                    MetricManager.INSTANCE.getCounter(tx.getConfiguration().getGroupName(), M_LOCKS, M_DELETE, M_CALLS).inc();
                }
            }
            // Regardless of whether we successfully deleted the lock from storage, take it out of the local mediator
            llm.unlock(kc, tx);
            iterator.remove();
        }
    }

    private boolean lockLocally(KeyColumn lockID, StoreTransaction tx) {
        return lockLocally(lockID, times.getTime().plus(lockExpire), tx);
    }

    private boolean lockLocally(KeyColumn lockID, Instant expire, StoreTransaction tx) {
        return llm.lock(lockID, tx, expire);
    }

    private void unlockLocally(KeyColumn lockID, StoreTransaction txh) {
        llm.unlock(lockID, txh);
    }
}
