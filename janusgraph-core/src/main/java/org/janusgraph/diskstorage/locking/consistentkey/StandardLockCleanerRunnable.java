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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_END;
import static org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_START;

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
    private final TimestampProvider times;

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

        List<StaticBuffer> deletions = new LinkedList<>();

        for (Entry lc : locks) {
            TimestampRid tr = serializer.fromLockColumn(lc.getColumn(), times);
            if (tr.getTimestamp().isBefore(cutoff)) {
                log.info("Deleting expired lock on {} by rid {} with timestamp {} (before or at cutoff {})",
                    target, tr.getRid(), tr.getTimestamp(), cutoff);
                deletions.add(lc.getColumn());
            } else {
                log.debug("Ignoring lock on {} by rid {} with timestamp {} (timestamp is after cutoff {})",
                    target, tr.getRid(), tr.getTimestamp(), cutoff);
            }
        }

        deletions = Collections.unmodifiableList(deletions);

        if (!deletions.isEmpty()) {
            store.mutate(lockKey, Collections.emptyList(), deletions, tx);
            log.info("Deleted {} expired locks (before or at cutoff {})", deletions.size(), cutoff);
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
            return other.tx == null;
        } else {
            return tx.equals(other.tx);
        }
    }
}
