package com.thinkaurelius.titan;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InfinispanStorageSetup extends StorageSetup {

    public static Configuration getInfinispanCacheGraphConfig(boolean transactional) {
        BaseConfiguration config = new BaseConfiguration();

        Configuration storage = config.subset(STORAGE_NAMESPACE);
        storage.addProperty(STORAGE_BACKEND_KEY, "infinispan");
        storage.addProperty(STORAGE_TRANSACTIONAL_KEY, transactional);
        return config;
    }

    public static Configuration getInfinispanBaseConfig(boolean transactional) {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(STORAGE_TRANSACTIONAL_KEY, transactional);
        return config;
    }
}
