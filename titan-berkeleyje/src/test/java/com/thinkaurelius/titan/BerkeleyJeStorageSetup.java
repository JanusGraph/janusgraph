package com.thinkaurelius.titan;


import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

public class BerkeleyJeStorageSetup extends StorageSetup {

    public static Configuration getBerkeleyJEStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, getHomeDir());
        return config;
    }

    public static Configuration getBerkeleyJEGraphConfiguration() {
        return getBerkeleyJEGraphBaseConfiguration();
    }

    public static Configuration getBerkeleyJEPerformanceConfiguration() {
        Configuration config = getBerkeleyJEGraphBaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY, false);
        config.addProperty(GraphDatabaseConfiguration.TX_CACHE_SIZE_KEY, 1000);
        return config;
    }

    public static Configuration getBerkeleyJEGraphBaseConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, getHomeDir());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "berkeleyje");
        return config;
    }


}
