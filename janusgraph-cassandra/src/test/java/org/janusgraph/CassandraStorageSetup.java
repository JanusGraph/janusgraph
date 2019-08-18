// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;

import org.janusgraph.diskstorage.cassandra.utils.CassandraDaemonWrapper;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class CassandraStorageSetup {

    public static final String CONFDIR_SYSPROP = "test.cassandra.confdir";
    public static final String DATADIR_SYSPROP = "test.cassandra.datadir";
    public static final String HOSTNAME = System.getProperty(ConfigElement.getPath(STORAGE_HOSTS));

    private static volatile Paths paths;

    private static final Logger log = LoggerFactory.getLogger(CassandraStorageSetup.class);

    private static synchronized Paths getPaths() {
        if (null == paths) {
            String yamlPath = "file://" + loadAbsoluteDirectoryPath("conf", CONFDIR_SYSPROP, true) + File.separator + "cassandra.yaml";
            String dataPath = loadAbsoluteDirectoryPath("data", DATADIR_SYSPROP, false);
            paths = new Paths(yamlPath, dataPath);
        }
        return paths;
    }

    private static ModifiableConfiguration getGenericConfiguration(String ks, String backend) {
        ModifiableConfiguration config = buildGraphConfiguration();
        if (null != ks) {
            config.set(CASSANDRA_KEYSPACE, cleanKeyspaceName(ks));
            log.debug("Set keyspace name: {}", config.get(CASSANDRA_KEYSPACE));
        }
        config.set(PAGE_SIZE,500);
        config.set(CONNECTION_TIMEOUT, Duration.ofSeconds(60L));
        config.set(STORAGE_BACKEND, backend);
        if (HOSTNAME != null) config.set(STORAGE_HOSTS, new String[]{HOSTNAME});
        config.set(DROP_ON_CLEAR, false);
        return config;
    }


    public static ModifiableConfiguration getEmbeddedConfiguration(String ks) {
        ModifiableConfiguration config = getGenericConfiguration(ks, "embeddedcassandra");
        config.set(STORAGE_CONF_FILE, getPaths().yamlPath);
        return config;
    }

    public static ModifiableConfiguration getEmbeddedCassandraPartitionConfiguration(String ks) {
        ModifiableConfiguration config = getEmbeddedConfiguration(ks);
        config.set(IDS_FLUSH,false);
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

    public static ModifiableConfiguration getAstyanaxSSLConfiguration(String ks) {
        return enableSSL(getGenericConfiguration(ks, "astyanax"));
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

    public static ModifiableConfiguration getCassandraThriftSSLConfiguration(String ks) {
        return enableSSL(getGenericConfiguration(ks, "cassandrathrift"));
    }

    public static WriteConfiguration getCassandraThriftGraphConfiguration(String ks) {
        return getCassandraThriftConfiguration(ks).getConfiguration();
    }

    public static ModifiableConfiguration getEmbeddedOrThriftConfiguration(String keyspace) {
        final ModifiableConfiguration config;
        if (HOSTNAME == null) {
            config = getEmbeddedConfiguration(keyspace);
        }  else {
            config = getCassandraThriftConfiguration(keyspace);
        }
        return config;
    }

    /**
     * Load cassandra.yaml and data paths from the environment or from default
     * values if nothing is set in the environment, then delete all existing
     * data, and finally start Cassandra.
     * <p>
     * This method is idempotent. Calls after the first have no effect aside
     * from logging statements.
     */
    public static void startCleanEmbedded() {
        if (HOSTNAME == null) {
            startCleanEmbedded(getPaths());
        }
    }

    /*
     * Cassandra only accepts keyspace names 48 characters long or shorter made
     * up of alphanumeric characters and underscores.
     */
    public static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());

        if (48 < raw.length() || raw.matches("^.*[^a-zA-Z0-9_].*$")) {
            return "strhash" + Math.abs(raw.hashCode());
        } else {
            return raw;
        }
    }

    private static ModifiableConfiguration enableSSL(ModifiableConfiguration mc) {
        mc.set(AbstractCassandraStoreManager.SSL_ENABLED, true);
        mc.set(STORAGE_HOSTS, new String[] {HOSTNAME != null ? HOSTNAME : "127.0.0.1"});
        mc.set(AbstractCassandraStoreManager.SSL_TRUSTSTORE_LOCATION,
                Joiner.on(File.separator).join("target", "cassandra", "conf", "localhost-murmur-ssl", "test.truststore"));
        mc.set(AbstractCassandraStoreManager.SSL_TRUSTSTORE_PASSWORD, "cassandra");
        return mc;
    }

    private static void startCleanEmbedded(Paths p) {
        if (!CassandraDaemonWrapper.isStarted()) {
            try {
                FileUtils.deleteDirectory(new File(p.dataPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        CassandraDaemonWrapper.start(p.yamlPath);
    }

    private static String loadAbsoluteDirectoryPath(String name, String prop, boolean mustExistAndBeAbsolute) {
        String s = System.getProperty(prop);

        if (null == s) {
            s = Joiner.on(File.separator).join(System.getProperty("user.dir"), "target", "cassandra", name, "localhost-bop");
            log.info("Set default Cassandra {} directory path {}", name, s);
        } else {
            log.info("Loaded Cassandra {} directory path {} from system property {}", name, s, prop);
        }

        if (mustExistAndBeAbsolute) {
            File dir = new File(s);
            Preconditions.checkArgument(dir.isDirectory(), "Path %s must be a directory", s);
            Preconditions.checkArgument(dir.isAbsolute(),  "Path %s must be absolute", s);
        }

        return s;
    }

    private static class Paths {
        private final String yamlPath;
        private final String dataPath;

        public Paths(String yamlPath, String dataPath) {
            this.yamlPath = yamlPath;
            this.dataPath = dataPath;
        }
    }
}
