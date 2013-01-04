package com.thinkaurelius.titan;


import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import java.io.File;

public class CassandraStorageSetup {

    public static final String cassandraYamlPath = StringUtils.join(
            new String[]{"file://", System.getProperty("user.dir"), "titan-cassandra", "target",
                    "cassandra-tmp", "conf", "127.0.0.1", "cassandra.yaml"},
            File.separator);
    public static final String cassandraOrderedYamlPath = StringUtils.join(
            new String[]{"file://", System.getProperty("user.dir"), "titan-cassandra", "target",
                    "cassandra-tmp", "conf", "127.0.0.1", "cassandra-ordered.yaml"},
            File.separator);


    public static Configuration getCassandraStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        return config;

    }

    public static Configuration getEmbeddedCassandraStorageConfiguration(boolean ordered) {
        Configuration config = getCassandraStorageConfiguration();
        if (ordered)
            config.addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                cassandraOrderedYamlPath);
        else
            config.addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                cassandraYamlPath);
        return config;
    }


    public static Configuration getAstyanaxGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "astyanax");
        return config;
    }

    public static Configuration getCassandraGraphConfiguration() {
        Configuration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "cassandra");
        return config;
    }

    public static Configuration getCassandraThriftGraphConfiguration() {
        Configuration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "cassandrathrift");
        return config;
    }

    public static Configuration getEmbeddedCassandraGraphConfiguration() {
        Configuration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "embeddedcassandra");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                cassandraYamlPath);
        return config;
    }

    public static Configuration getEmbeddedCassandraPartitionGraphConfiguration() {
        Configuration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "embeddedcassandra");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                cassandraOrderedYamlPath);
        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_PARTITION_KEY, true);
        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_FLUSH_KEY, false);
        return config;
    }
}
