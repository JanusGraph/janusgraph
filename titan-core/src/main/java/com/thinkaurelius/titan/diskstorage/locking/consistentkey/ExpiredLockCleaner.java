package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

import static com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_START;
import static com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_END;

public class ExpiredLockCleaner implements Runnable {

    private final KeyColumnValueStore store;
    private final KeyColumn deletionTarget;
    private final ConsistentKeyLockerSerializer serializer;
    private final TimestampProvider times;
    private final StoreTransaction tx;
    private final long cutoff;

    private static final Logger log = LoggerFactory.getLogger(ExpiredLockCleaner.class);

    public ExpiredLockCleaner(KeyColumnValueStore store, KeyColumn deletionTarget, TimestampProvider times, StoreTransaction tx, ConsistentKeyLockerSerializer serializer, long cutoff) {
        this.store = store;
        this.deletionTarget = deletionTarget;
        this.serializer = serializer;
        this.times = times;
        this.tx = tx;
        this.cutoff = cutoff;
    }

    @Override
    public void run() {
        try {
            runWithExceptions();
        } catch (StorageException e) {
            log.warn("Expired lock cleaner failed", e);
        }
    }

    private void runWithExceptions() throws StorageException {
        StaticBuffer lockKey = serializer.toLockKey(deletionTarget.getKey(), deletionTarget.getColumn());
        List<Entry> locks = store.getSlice(new KeySliceQuery(lockKey, LOCK_COL_START, LOCK_COL_END), tx); // TODO reduce LOCK_COL_END based on cutoff

        ImmutableList.Builder<StaticBuffer> b = ImmutableList.builder();

        for (Entry lc : locks) {
            TimestampRid tr = serializer.fromLockColumn(lc.getColumn());
            if (tr.getTimestamp() <= cutoff) {
                log.info("Deleting expired lock on {} by rid {} with timestamp {} (before or at cutoff {})",
                        new Object[] { deletionTarget, tr.getRid(), tr.getTimestamp(), cutoff });
                b.add(lc.getColumn());
            } else {
                log.debug("Ignoring lock on {} by rid {} with timestamp {} (timestamp is after cutoff {})",
                        new Object[] { deletionTarget, tr.getRid(), tr.getTimestamp(), cutoff });
            }
        }

        List<StaticBuffer> dels = b.build();

        if (!dels.isEmpty()) {
            store.mutate(lockKey, ImmutableList.<Entry>of(), dels, tx);
        }
    }
}
