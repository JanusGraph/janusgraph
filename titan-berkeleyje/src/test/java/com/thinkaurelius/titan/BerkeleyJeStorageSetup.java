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
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, getHomeDir());
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "berkeleyje");
//        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_FLUSH_KEY, false);
        return config;
    }



}
