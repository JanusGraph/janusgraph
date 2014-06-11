package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class BerkeleyStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getBerkeleyJEConfiguration(String dir) {
        return buildConfiguration()
                .set(STORAGE_BACKEND,"berkeleyje")
                .set(STORAGE_DIRECTORY, dir);
    }

    public static ModifiableConfiguration getBerkeleyJEConfiguration() {
        return getBerkeleyJEConfiguration(getHomeDir());
    }

    public static WriteConfiguration getBerkeleyJEGraphConfiguration() {
        return getBerkeleyJEConfiguration().getConfiguration();
    }

    public static ModifiableConfiguration getBerkeleyJEPerformanceConfiguration() {
        return getBerkeleyJEConfiguration()
                .set(STORAGE_TRANSACTIONAL,false)
                .set(TX_CACHE_SIZE,1000);
    }
}
