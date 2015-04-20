package com.thinkaurelius.titan.diskstorage.common;


import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;

import java.util.List;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Abstract Store Manager used as the basis for concrete StoreManager implementations.
 * Simplifies common configuration management.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractStoreManager implements StoreManager {

    protected final boolean transactional;
    protected final boolean batchLoading;
    protected final Configuration storageConfig;

    public AbstractStoreManager(Configuration storageConfig) {
        batchLoading = storageConfig.get(STORAGE_BATCH);
        boolean transactional = storageConfig.get(STORAGE_TRANSACTIONAL);
        if (batchLoading) {
            transactional = false;
        }
        this.transactional = transactional;
        this.storageConfig = storageConfig;
    }

    public Configuration getStorageConfig() {
        return storageConfig;
    }

    public EntryMetaData[] getMetaDataSchema(String storeName) {
        List<EntryMetaData> schemaBuilder = Lists.newArrayList();
        StoreFeatures features = getFeatures();
        if (features.hasTimestamps() && storageConfig.get(STORE_META_TIMESTAMPS,storeName))
            schemaBuilder.add(EntryMetaData.TIMESTAMP);
        if (features.hasCellTTL() && storageConfig.get(STORE_META_TTL,storeName))
            schemaBuilder.add(EntryMetaData.TTL);
        if (features.hasVisibility() && storageConfig.get(STORE_META_VISIBILITY,storeName))
            schemaBuilder.add(EntryMetaData.VISIBILITY);

        if (schemaBuilder.isEmpty()) return StaticArrayEntry.EMPTY_SCHEMA;
        return schemaBuilder.toArray(new EntryMetaData[schemaBuilder.size()]);
    }

}
