package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class PersistitStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getPersistitConfig() {
        return getPersistitConfig(getHomeDir());
    }

    public static ModifiableConfiguration getPersistitConfig(String dir) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"persistit");
        config.set(GraphDatabaseConfiguration.STORAGE_DIRECTORY, dir);
        return config;
    }

    public static WriteConfiguration getPersistitGraphConfig() {
        return getPersistitConfig().getConfiguration();
    }

}
