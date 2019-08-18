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

package org.janusgraph.diskstorage.cql;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_LOCATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.CONNECTION_TIMEOUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.cassandra.utils.CassandraDaemonWrapper;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class CassandraStorageSetup {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorageSetup.class);

    public static final String CONFDIR_SYSPROP = "test.cassandra.confdir";
    public static final String DATADIR_SYSPROP = "test.cassandra.datadir";
    public static final String HOSTNAME = System.getProperty(ConfigElement.getPath(STORAGE_HOSTS));

    static {
        setWrapperStoreManager();
    }

    private static volatile Paths paths;

    /**
     * Load cassandra.yaml and data paths from the environment or from default values if nothing is set in the environment, then delete all
     * existing data, and finally start Cassandra.
     * <p>
     * This method is idempotent. Calls after the first have no effect aside from logging statements.
     */
    public static void startCleanEmbedded() {
        if (HOSTNAME == null) {
            final Paths p = getPaths();
            if (!CassandraDaemonWrapper.isStarted()) {
                try {
                    FileUtils.deleteDirectory(new File(p.dataPath));
                    FileUtils.deleteQuietly(new File((new File(p.dataPath)).getParent() + File.separator + "commitlog"));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
            CassandraDaemonWrapper.start(p.yamlPath);
        }
    }

    private static synchronized Paths getPaths() {
        if (null == paths) {
            final String yamlPath = "file://" + loadAbsoluteDirectoryPath("conf", CONFDIR_SYSPROP, true) + File.separator
                    + "cassandra.yaml";
            final String dataPath = loadAbsoluteDirectoryPath("data", DATADIR_SYSPROP, false);
            paths = new Paths(yamlPath, dataPath);
        }
        return paths;
    }

    private static String loadAbsoluteDirectoryPath(final String name, final String prop, final boolean mustExistAndBeAbsolute) {
        String s = System.getProperty(prop);

        if (null == s) {
            s = Joiner.on(File.separator).join(System.getProperty("user.dir"), "target", "cassandra", "byteorderedpartitioner", name);
            LOGGER.info("Set default Cassandra {} directory path {}", name, s);
        } else {
            LOGGER.info("Loaded Cassandra {} directory path {} from system property {}", name, s, prop);
        }

        if (mustExistAndBeAbsolute) {
            final File dir = new File(s);
            Preconditions.checkArgument(dir.isDirectory(), "Path %s must be a directory", s);
            Preconditions.checkArgument(dir.isAbsolute(), "Path %s must be absolute", s);
        }

        return s;
    }

    public static ModifiableConfiguration getCQLConfiguration(final String keyspace) {
        final ModifiableConfiguration config = buildGraphConfiguration();
        config.set(KEYSPACE, cleanKeyspaceName(keyspace));
        LOGGER.debug("Set keyspace name: {}", config.get(KEYSPACE));
        config.set(PAGE_SIZE, 500);
        config.set(CONNECTION_TIMEOUT, Duration.ofSeconds(60L));
        config.set(STORAGE_BACKEND, "cql");
        if (HOSTNAME != null) config.set(STORAGE_HOSTS, new String[]{HOSTNAME});
        config.set(DROP_ON_CLEAR, false);
        return config;
    }

    public static ModifiableConfiguration enableSSL(final ModifiableConfiguration mc) {
        mc.set(SSL_ENABLED, true);
        mc.set(STORAGE_HOSTS, new String[]{HOSTNAME != null ? HOSTNAME : "127.0.0.1"});
        mc.set(SSL_TRUSTSTORE_LOCATION,
                Joiner.on(File.separator).join("target", "cassandra", "murmur-ssl", "conf", "test.truststore"));
        mc.set(SSL_TRUSTSTORE_PASSWORD, "cassandra");
        return mc;
    }

    /**
     * Cassandra only accepts keyspace names 48 characters long or shorter made up of alphanumeric characters and underscores.
     */
    private static String cleanKeyspaceName(final String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());

        if (48 < raw.length() || raw.matches("^.*[^a-zA-Z0-9_].*$")) {
            return "strhash" + Math.abs(raw.hashCode());
        } else {
            return raw;
        }
    }

    private static void setWrapperStoreManager() {
        try {
            final Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);

            Field field = StandardStoreManager.class.getDeclaredField("managerClass");
            field.setAccessible(true);
            field.set(StandardStoreManager.CQL, CachingCQLStoreManager.class.getCanonicalName());

            field = StandardStoreManager.class.getDeclaredField("ALL_SHORTHANDS");
            field.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, ImmutableList.copyOf(StandardStoreManager.CQL.getShorthands()));

            field = StandardStoreManager.class.getDeclaredField("ALL_MANAGER_CLASSES");
            field.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, ImmutableMap.of(StandardStoreManager.CQL.getShorthands().get(0), StandardStoreManager.CQL.getManagerClass()));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to set wrapper CQL store manager", e);
        }
    }

    private static class Paths {

        private final String yamlPath;
        private final String dataPath;

        public Paths(final String yamlPath, final String dataPath) {
            this.yamlPath = yamlPath;
            this.dataPath = dataPath;
        }
    }
}
