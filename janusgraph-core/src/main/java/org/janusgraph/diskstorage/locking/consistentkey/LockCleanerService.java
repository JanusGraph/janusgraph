package org.janusgraph.diskstorage.locking.consistentkey;

import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.KeyColumn;

import java.time.Instant;

public interface LockCleanerService {
    public void clean(KeyColumn target, Instant cutoff, StoreTransaction tx);
}
