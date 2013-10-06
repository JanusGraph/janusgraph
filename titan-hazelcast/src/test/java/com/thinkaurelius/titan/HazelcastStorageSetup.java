package com.thinkaurelius.titan;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class HazelcastStorageSetup extends StorageSetup {

    public static Configuration getHazelcastKCVSGraphConfig(boolean transactional) {
        BaseConfiguration config = new BaseConfiguration();

        Configuration storage = config.subset(STORAGE_NAMESPACE);
        storage.addProperty(STORAGE_DIRECTORY_KEY, getHomeDir());
        storage.addProperty(STORAGE_BACKEND_KEY, "hazelcastkcvs");
        storage.addProperty(STORAGE_TRANSACTIONAL_KEY, transactional);
        return config;
    }

    public static Configuration getHazelcastCacheGraphConfig(boolean transactional) {
        BaseConfiguration config = new BaseConfiguration();

        Configuration storage = config.subset(STORAGE_NAMESPACE);
        storage.addProperty(STORAGE_DIRECTORY_KEY, getHomeDir());
        storage.addProperty(STORAGE_BACKEND_KEY, "hazelcastcache");
        storage.addProperty(STORAGE_TRANSACTIONAL_KEY, transactional);
        return config;
    }


    public static Configuration getHazelcastBaseConfig() {
        return getHazelcastBaseConfig(true);
    }

    public static Configuration getHazelcastBaseConfig(boolean transactional) {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(STORAGE_DIRECTORY_KEY, getHomeDir());
        config.addProperty(STORAGE_TRANSACTIONAL_KEY, transactional);
        return config;
    }
}
