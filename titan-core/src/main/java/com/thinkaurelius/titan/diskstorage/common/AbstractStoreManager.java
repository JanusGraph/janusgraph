package com.thinkaurelius.titan.diskstorage.common;


import com.thinkaurelius.titan.diskstorage.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Abstract Store Manager used as the basis for concrete StoreManager implementations.
 * Simplifies common configuration management.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractStoreManager {

    protected final boolean transactional;
    protected final boolean isReadOnly;
    protected final boolean batchLoading;

    public AbstractStoreManager(Configuration storageConfig) {
        isReadOnly = storageConfig.get(STORAGE_READONLY);
        batchLoading = storageConfig.get(STORAGE_BATCH);
        boolean transactional = storageConfig.get(STORAGE_TRANSACTIONAL);
        if (batchLoading) {
            transactional = false;
        }
        this.transactional = transactional;
    }

}
