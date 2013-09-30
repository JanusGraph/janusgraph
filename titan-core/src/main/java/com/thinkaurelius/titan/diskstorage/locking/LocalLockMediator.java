package com.thinkaurelius.titan.diskstorage.locking;

import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class resolves lock contention between two transactions on the same JVM.
 * <p/>
 * This is not just an optimization to reduce network traffic. Locks written by
 * Titan to a distributed key-value store contain an identifier, the "Rid",
 * which is unique only to the process level. The Rid can't tell which
 * transaction in a process holds any given lock. This class prevents two
 * transactions in a single process from concurrently writing the same lock to a
 * distributed key-value store.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */

public class LocalLockMediator<T> {

    private static final Logger log = LoggerFactory
            .getLogger(LocalLockMediator.class);

    /**
     * Namespace for which this mediator is responsible
     *
     * @see LocalLockMediatorProvider
     */
    private final String name;

    /**
     * Maps a ({@code key}, {@code column}) pair to the local transaction
     * holding a lock on that pair. Values in this map may have already expired
     * according to {@link AuditRecord#expires}, in which case the lock should
     * be considered invalid.
     */
    private final ConcurrentHashMap<KeyColumn, AuditRecord<T>> locks = new ConcurrentHashMap<KeyColumn, AuditRecord<T>>();

    public LocalLockMediator(String name) {
        this.name = name;

        assert null != this.name;
    }

    /**
     * Acquire the lock specified by {@code kc}.
     * <p/>
     * <p/>
     * For any particular key-column, whatever value of {@code requestor} is
     * passed to this method must also be passed to the associated later call to
     * {@link #unlock(KeyColumn, ExpectedValueCheckingTransaction)}.
     * <p/>
     * If some requestor {@code r} calls this method on a KeyColumn {@code k}
     * and this method returns true, then subsequent calls to this method by
     * {@code r} on {@code l} merely attempt to update the {@code expiresAt}
     * timestamp. This differs from typical lock reentrance: multiple successful
     * calls to this method do not require an equal number of calls to
     * {@code #unlock()}. One {@code #unlock()} call is enough, no matter how
     * many times a {@code requestor} called {@code lock} beforehand. Note that
     * updating the timestamp may fail, in which case the lock is considered to
     * have expired and the calling context should assume it no longer holds the
     * lock specified by {@code kc}.
     * <p/>
     * The number of nanoseconds elapsed since the UNIX Epoch is not readily
     * available within the JVM. When reckoning expiration times, this method
     * uses the approximation implemented by
     * {@link com.thinkaurelius.titan.diskstorage.util.TimeUtility#getApproxNSSinceEpoch(false)}.
     * <p/>
     * The current implementation of this method returns true when given an
     * {@code expiresAt} argument in the past. Future implementations may return
     * false instead.
     *
     * @param kc        lock identifier
     * @param requestor the object locking {@code kc}
     * @param expires   the absolute time since the UNIX epoch at which this lock will automatically expire
     * @param tu        the units of {@code expires}
     * @return true if the lock is acquired, false if it was not acquired
     */
    public boolean lock(KeyColumn kc, T requestor,
                        long expires, TimeUnit tu) {
        assert null != kc;
        assert null != requestor;

        AuditRecord<T> audit = new AuditRecord<T>(requestor, TimeUnit.NANOSECONDS.convert(expires, tu));
        AuditRecord<T> inmap = locks.putIfAbsent(kc, audit);

        boolean success = false;

        if (null == inmap) {
            // Uncontended lock succeeded
            if (log.isTraceEnabled()) {
                log.trace("New local lock created: {} namespace={} txn={}",
                        new Object[]{kc, name, requestor});
            }
            success = true;
        } else if (inmap.equals(audit)) {
            // requestor has already locked kc; update expiresAt
            success = locks.replace(kc, inmap, audit);
            if (log.isTraceEnabled()) {
                if (success) {
                    log.trace(
                            "Updated local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
                            new Object[]{kc, name, requestor, inmap.expires,
                                    audit.expires});
                } else {
                    log.trace(
                            "Failed to update local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
                            new Object[]{kc, name, requestor, inmap.expires,
                                    audit.expires});
                }
            }
        } else if (inmap.expires <= TimeUtility.INSTANCE.getApproxNSSinceEpoch()) {
            // the recorded lock has expired; replace it
            success = locks.replace(kc, inmap, audit);
            if (log.isTraceEnabled()) {
                log.trace(
                        "Discarding expired lock: {} namespace={} txn={} expired={}",
                        new Object[]{kc, name, inmap.holder, inmap.expires});
            }
        } else {
            // we lost to a valid lock
            if (log.isTraceEnabled()) {
                log.trace(
                        "Local lock failed: {} namespace={} txn={} (already owned by {})",
                        new Object[]{kc, name, requestor, inmap});
            }
        }

        return success;
    }

    /**
     * Release the lock specified by {@code kc} and which was previously
     * locked by {@code requestor}, if it is possible to release it.
     *
     * @param kc        lock identifier
     * @param requestor the object which previously locked {@code kc}
     */
    public boolean unlock(KeyColumn kc, T requestor) {

        if (!locks.containsKey(kc)) {
            log.error("Local unlock failed: no locks found for {}", kc);
            return false;
        }

        AuditRecord<T> unlocker = new AuditRecord<T>(requestor, 0);

        AuditRecord<T> holder = locks.get(kc);

        if (!holder.equals(unlocker)) {
            log.error("Local unlock of {} by {} failed: it is held by {}",
                    new Object[]{kc, unlocker, holder});
            return false;
        }

        boolean removed = locks.remove(kc, unlocker);

        if (removed) {
            if (log.isTraceEnabled()) {
                log.trace("Local unlock succeeded: {} namespace={} txn={}",
                        new Object[]{kc, name, requestor});
            }
        } else {
            log.warn("Local unlock warning: lock record for {} disappeared "
                    + "during removal; this suggests the lock either expired "
                    + "while we were removing it, or that it was erroneously "
                    + "unlocked multiple times.", kc);
        }

        // Even if !removed, we're finished unlocking, so return true
        return true;
    }

    public String toString() {
        return "LocalLockMediator [" + name + ",  ~" + locks.size()
                + " current locks]";
    }

    /**
     * A record containing the local transaction that holds a lock and the
     * lock's expiration time.
     */
    private static class AuditRecord<T> {

        /**
         * The local transaction that holds/held the lock.
         */
        private final T holder;
        /**
         * The expiration time of a the lock. Conventionally, this is in
         * nanoseconds from the epoch as returned by
         * {@link TimeUtility#getApproxNSSinceEpoch()}.
         */
        private final long expires;
        /**
         * Cached hashCode.
         */
        private int hashCode;

        private AuditRecord(T holder, long expires) {
            this.holder = holder;
            this.expires = expires;
        }

        /**
         * This implementation depends only on the lock holder and not on the
         * lock expiration time.
         */
        @Override
        public int hashCode() {
            if (0 == hashCode)
                hashCode = holder.hashCode();

            return hashCode;
        }

        /**
         * This implementation depends only on the lock holder and not on the
         * lock expiration time.
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            /*
             * This warning suppression is harmless because we are only going to
             * call other.holder.equals(...), and since equals(...) is part of
             * Object, it is guaranteed to be defined no matter the concrete
             * type of parameter T.
             */
            @SuppressWarnings("rawtypes")
            AuditRecord other = (AuditRecord) obj;
            if (holder == null) {
                if (other.holder != null)
                    return false;
            } else if (!holder.equals(other.holder))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "AuditRecord [txn=" + holder + ", expires=" + expires + "]";
        }

    }

}
