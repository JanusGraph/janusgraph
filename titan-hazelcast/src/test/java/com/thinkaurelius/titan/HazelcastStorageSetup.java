package com.thinkaurelius.titan;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class HazelcastStorageSetup extends StorageSetup {
    public static Configuration getHazelcastGraphConfig() {
        BaseConfiguration config = new BaseConfiguration();

        Configuration storage = config.subset(STORAGE_NAMESPACE);
        {
            storage.addProperty(STORAGE_DIRECTORY_KEY, getHomeDir());
            storage.addProperty(STORAGE_BACKEND_KEY, "hazelcast");
        }

        return config;
    }
}
