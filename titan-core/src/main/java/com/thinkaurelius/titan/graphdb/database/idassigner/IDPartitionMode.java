package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;

public enum IDPartitionMode {
    /**
     * Automatically enable partitioning when the storage backend reports true
     * for both {@link StoreFeatures#isDistributed()} and
     * {@link StoreFeatures#isKeyOrdered()}.
     */
    DEFAULT,
    
    /**
     * Force partitioning on.
     */
    ENABLED,
    
    /**
     * Force partitioning off.
     */
    DISABLED;
}
