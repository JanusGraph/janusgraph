package com.thinkaurelius.titan;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

public class HazelcastStorageSetup extends StorageSetup {
    public static Configuration getHazelcastGraphConfig() {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, getHomeDir());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hazelcast");
        return storage;
    }

}
