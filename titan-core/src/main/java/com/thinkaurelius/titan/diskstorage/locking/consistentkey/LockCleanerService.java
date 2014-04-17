package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

public interface LockCleanerService {

    /**
     * Cleans out the target lock application with the specified cutoff (in {@link com.thinkaurelius.titan.diskstorage.time.Timestamps#SYSTEM#getUnit()})
     * @param target
     * @param cutoff
     * @param tx
     */
    public void clean(KeyColumn target, long cutoff, StoreTransaction tx);

}
