package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

public interface LockCleanerService {
    public void clean(KeyColumn target, long cutoff, StoreTransaction tx);
}
