package com.thinkaurelius.titan;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.KEYSPACE_KEY;

import java.io.File;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class CassandraStorageSetup {

    public static final String CONFDIR_SYSPROP = "test.cassandra.confdir";
    public static final String DATADIR_SYSPROP = "test.cassandra.datadir";
    public static final String YAML_PATH;
    public static final String DATA_PATH;

    private static final Logger log = LoggerFactory.getLogger(CassandraStorageSetup.class);
    
    static {
        YAML_PATH = "file://" + loadAbsoluteDirectoryPath("conf", CONFDIR_SYSPROP, true) + File.separator + "cassandra.yaml";
        DATA_PATH = loadAbsoluteDirectoryPath("data", DATADIR_SYSPROP, false);
    }
    
    public static Configuration getGenericCassandraStorageConfiguration(String ks) {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(KEYSPACE_KEY, cleanKeyspaceName(ks));
        config.addProperty(GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY, 60000L);
        return config;
        
    }
    
    public static Configuration getEmbeddedCassandraStorageConfiguration(String ks) {
        Configuration config = getGenericCassandraStorageConfiguration(ks);
        config.addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                YAML_PATH);
        return config;
    }

    public static Configuration getAstyanaxGraphConfiguration(String ks) {
        return getGraphBaseConfiguration(ks, "astyanax");
    }

    public static Configuration getCassandraGraphConfiguration(String ks) {
        return getGraphBaseConfiguration(ks, "cassandra");
    }

    public static Configuration getCassandraThriftGraphConfiguration(String ks) {
        return getGraphBaseConfiguration(ks, "cassandrathrift");
    }

    public static Configuration getEmbeddedCassandraGraphConfiguration(String ks) {
        Configuration config = getGraphBaseConfiguration(ks, "embeddedcassandra");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                YAML_PATH);
        return config;
    }

    public static Configuration getEmbeddedCassandraPartitionGraphConfiguration(String ks) {
        Configuration config = getGraphBaseConfiguration(ks, "embeddedcassandra");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                YAML_PATH);
        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_PARTITION_KEY, true);
        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_FLUSH_KEY, false);
//        config.subset(GraphDatabaseConfiguration.METRICS_NAMESPACE).addProperty(GraphDatabaseConfiguration.METRICS_CONSOLE_INTERVAL, 3000L);
        return config;
    }
    
    /*
     * Cassandra only accepts keyspace names 48 characters long or shorter made
     * up of alphanumeric characters and underscores.
     */
    private static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());
        
        if (48 < raw.length() || raw.matches("[^a-zA-Z_]")) {
            return "strhash" + String.valueOf(Math.abs(raw.hashCode()));
        } else {
            return raw;
        }
    }
    
    private static Configuration getGraphBaseConfiguration(String ks, String backend) {
        Configuration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(KEYSPACE_KEY, cleanKeyspaceName(ks));
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, backend);
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY, 60000L);
        return config;
    }
    
    private static String loadAbsoluteDirectoryPath(String name, String prop, boolean mustExistAndBeAbsolute) {
        String s = System.getProperty(prop);
        
        if (null == s) {
            s = Joiner.on(File.separator).join(System.getProperty("user.dir"), "target", "cassandra", name, "localhost-rp");
            log.info("Set default Cassandra {} directory path {}", name, s);
        } else {
            log.info("Loaded Cassandra {} directory path {} from system property {}", new Object[] { name, s, prop });
        }

        if (mustExistAndBeAbsolute) {
            File dir = new File(s);
            Preconditions.checkArgument(dir.isDirectory(), "Path %s must be a directory", s);
            Preconditions.checkArgument(dir.isAbsolute(),  "Path %s must be absolute", s);
        }
        
        return s;
    }
}
