package com.thinkaurelius.titan;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.CLUSTER_PARTITION;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

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

    private static ModifiableConfiguration getGenericConfiguration(String ks, String backend) {
        ModifiableConfiguration config = buildConfiguration();
        config.set(CASSANDRA_KEYSPACE, cleanKeyspaceName(ks));
        config.set(PAGE_SIZE,500);
        config.set(CONNECTION_TIMEOUT, new StandardDuration(60L, TimeUnit.SECONDS));
        config.set(STORAGE_BACKEND, backend);
        return config;
    }


    public static ModifiableConfiguration getEmbeddedConfiguration(String ks) {
        ModifiableConfiguration config = getGenericConfiguration(ks, "embeddedcassandra");
        config.set(STORAGE_CONF_FILE,YAML_PATH);
        return config;
    }

    public static ModifiableConfiguration getEmbeddedCassandraPartitionConfiguration(String ks) {
        ModifiableConfiguration config = getEmbeddedConfiguration(ks);
        config.set(CLUSTER_PARTITION, true);
        config.set(IDS_FLUSH,false);
//        config.subset(GraphDatabaseConfiguration.METRICS_NAMESPACE).addProperty(GraphDatabaseConfiguration.METRICS_CONSOLE_INTERVAL, 3000L);
        return config;
    }

    public static WriteConfiguration getEmbeddedGraphConfiguration(String ks) {
        return getEmbeddedConfiguration(ks).getConfiguration();
    }

    public static WriteConfiguration getEmbeddedCassandraPartitionGraphConfiguration(String ks) {
        return getEmbeddedConfiguration(ks).getConfiguration();
    }

    public static ModifiableConfiguration getAstyanaxConfiguration(String ks) {
        return getGenericConfiguration(ks, "astyanax");
    }

    public static WriteConfiguration getAstyanaxGraphConfiguration(String ks) {
        return getAstyanaxConfiguration(ks).getConfiguration();
    }

    public static ModifiableConfiguration getCassandraConfiguration(String ks) {
        return getGenericConfiguration(ks, "cassandra");
    }

    public static WriteConfiguration getCassandraGraphConfiguration(String ks) {
        return getCassandraConfiguration(ks).getConfiguration();
    }

    public static ModifiableConfiguration getCassandraThriftConfiguration(String ks) {
        return getGenericConfiguration(ks, "cassandrathrift");
    }

    public static WriteConfiguration getCassandraThriftGraphConfiguration(String ks) {
        return getCassandraThriftConfiguration(ks).getConfiguration();
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

    public static synchronized void startCleanEmbedded(String cassandraYamlPath) {
        if (!CassandraDaemonWrapper.isStarted()) {
            try {
                FileUtils.deleteDirectory(new File(CassandraStorageSetup.DATA_PATH));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        CassandraDaemonWrapper.start(cassandraYamlPath);
    }

    private static String loadAbsoluteDirectoryPath(String name, String prop, boolean mustExistAndBeAbsolute) {
        String s = System.getProperty(prop);

        if (null == s) {
            s = Joiner.on(File.separator).join(System.getProperty("user.dir"), "target", "cassandra", name, "localhost-bop");
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
