package com.thinkaurelius.titan.diskstorage.common;

import org.apache.commons.configuration.Configuration;

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
        isReadOnly = storageConfig.getBoolean(STORAGE_READONLY_KEY, STORAGE_READONLY_DEFAULT);
        batchLoading = storageConfig.getBoolean(STORAGE_BATCH_KEY, STORAGE_BATCH_DEFAULT);
        boolean transactional = storageConfig.getBoolean(STORAGE_TRANSACTIONAL_KEY, STORAGE_TRANSACTIONAL_DEFAULT);
        if (batchLoading) {
            transactional = false;
        }
        this.transactional = transactional;
    }

}
