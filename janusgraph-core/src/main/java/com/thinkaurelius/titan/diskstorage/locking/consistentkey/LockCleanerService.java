package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

import java.time.Instant;

public interface LockCleanerService {
    public void clean(KeyColumn target, Instant cutoff, StoreTransaction tx);
}
