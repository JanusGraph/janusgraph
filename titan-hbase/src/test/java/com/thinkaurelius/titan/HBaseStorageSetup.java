package com.thinkaurelius.titan;


import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.File;

public class HBaseStorageSetup {


    public static Configuration getHBaseStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
        return config;
    }

    public static Configuration getHBaseGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
        return config;
    }

}
