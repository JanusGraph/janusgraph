package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import java.time.Instant;
import java.util.List;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

import static com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_START;
import static com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_END;

/**
 * Attempt to delete locks before a configurable timestamp cutoff using the
 * provided store, transaction, and serializer.
 *
 * This implementation is "best-effort." If the store or transaction closes in
 * the middle of its operation, or if the backend emits a storage exception, it
 * will fail without retrying and log the exception.
 */
public class StandardLockCleanerRunnable implements Runnable {

    private final KeyColumnValueStore store;
    private final KeyColumn target;
    private final ConsistentKeyLockerSerializer serializer;
    private final StoreTransaction tx;
    private final Instant cutoff;
    private TimestampProvider times;

    private static final Logger log = LoggerFactory.getLogger(StandardLockCleanerRunnable.class);

    public StandardLockCleanerRunnable(KeyColumnValueStore store, KeyColumn target, StoreTransaction tx, ConsistentKeyLockerSerializer serializer, Instant cutoff, TimestampProvider times) {
        this.store = store;
        this.target = target;
        this.serializer = serializer;
        this.tx = tx;
        this.cutoff = cutoff;
        this.times = times;
    }

    @Override
    public void run() {
        try {
            runWithExceptions();
        } catch (BackendException e) {
            log.warn("Expired lock cleaner failed", e);
        }
    }

    private void runWithExceptions() throws BackendException {
        StaticBuffer lockKey = serializer.toLockKey(target.getKey(), target.getColumn());
        List<Entry> locks = store.getSlice(new KeySliceQuery(lockKey, LOCK_COL_START, LOCK_COL_END), tx); // TODO reduce LOCK_COL_END based on cutoff

        ImmutableList.Builder<StaticBuffer> b = ImmutableList.builder();

        for (Entry lc : locks) {
            TimestampRid tr = serializer.fromLockColumn(lc.getColumn(), times);
            if (tr.getTimestamp().isBefore(cutoff)) {
                log.info("Deleting expired lock on {} by rid {} with timestamp {} (before or at cutoff {})",
                        new Object[] { target, tr.getRid(), tr.getTimestamp(), cutoff });
                b.add(lc.getColumn());
            } else {
                log.debug("Ignoring lock on {} by rid {} with timestamp {} (timestamp is after cutoff {})",
                        new Object[] { target, tr.getRid(), tr.getTimestamp(), cutoff });
            }
        }

        List<StaticBuffer> dels = b.build();

        if (!dels.isEmpty()) {
            store.mutate(lockKey, ImmutableList.<Entry>of(), dels, tx);
            log.info("Deleted {} expired locks (before or at cutoff {})", dels.size(), cutoff);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cutoff == null) ? 0 : cutoff.hashCode());
        result = prime * result + ((serializer == null) ? 0 : serializer.hashCode());
        result = prime * result + ((store == null) ? 0 : store.hashCode());
        result = prime * result + ((target == null) ? 0 : target.hashCode());
        result = prime * result + ((tx == null) ? 0 : tx.hashCode());
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
        StandardLockCleanerRunnable other = (StandardLockCleanerRunnable) obj;
        if (cutoff == null) {
            if (other.cutoff != null)
                return false;
        } else if (!cutoff.equals(other.cutoff))
            return false;
        if (serializer == null) {
            if (other.serializer != null)
                return false;
        } else if (!serializer.equals(other.serializer))
            return false;
        if (store == null) {
            if (other.store != null)
                return false;
        } else if (!store.equals(other.store))
            return false;
        if (target == null) {
            if (other.target != null)
                return false;
        } else if (!target.equals(other.target))
            return false;
        if (tx == null) {
            if (other.tx != null)
                return false;
        } else if (!tx.equals(other.tx))
            return false;
        return true;
    }
}
