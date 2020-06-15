// Copyright 2019 JanusGraph Authors
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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.cql.CachingCQLStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.CassandraContainer;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Collections;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class JanusGraphCassandraContainer extends CassandraContainer<JanusGraphCassandraContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JanusGraphCassandraContainer.class);

    private static final String DEFAULT_VERSION = "3.11.6";
    private static final String DEFAULT_IMAGE = "cassandra";
    private static final String DEFAULT_PARTITIONER = "murmur";
    private static final boolean DEFAULT_USE_SSL = false;
    private static final boolean DEFAULT_ENABLE_CLIENT_AUTH = false;
    private static final boolean DEFAULT_USE_DEFAULT_CONFIG_FROM_IMAGE = false;

    static {
        setWrapperStoreManager();
    }

    private static String getVersion() {
        String property = System.getProperty("cassandra.docker.version");
        if (property != null && !property.isEmpty())
            return property;
        return DEFAULT_VERSION;
    }

    private static String getCassandraImage() {
        String property = System.getProperty("cassandra.docker.image");
        if (property != null && !property.isEmpty())
            return property;
        return DEFAULT_IMAGE;
    }

    private static String getPartitioner() {
        String property = System.getProperty("cassandra.docker.partitioner");
        if (property != null && !property.isEmpty())
            return property;
        return DEFAULT_PARTITIONER;
    }

    private static boolean useDynamicConfig() {
        String property = System.getProperty("cassandra.docker.useDefaultConfigFromImage");
        if (property != null && !property.isEmpty())
            return !Boolean.parseBoolean(property);
        return !DEFAULT_USE_DEFAULT_CONFIG_FROM_IMAGE;
    }

    private static boolean useSSL() {
        String property = System.getProperty("cassandra.docker.useSSL");
        if (property != null && !property.isEmpty())
            return Boolean.parseBoolean(property);
        return DEFAULT_USE_SSL;
    }

    private static boolean enableClientAuth() {
        String property = System.getProperty("cassandra.docker.enableClientAuth");
        if (property != null && !property.isEmpty())
            return Boolean.parseBoolean(property);
        return DEFAULT_ENABLE_CLIENT_AUTH;
    }

    private String getConfigPrefix() {
        return "cassandra3";
    }

    public JanusGraphCassandraContainer() {
        this(false);
    }

    public JanusGraphCassandraContainer(boolean bindDefaultPort) {
        super(getCassandraImage() + ":" + getVersion());
        if (bindDefaultPort) {
            addFixedExposedPort(CQL_PORT, CQL_PORT);
        }
        withEnv("MAX_HEAP_SIZE", "2G");
        withEnv("HEAP_NEWSIZE", "1G");

        if (useDynamicConfig()) {
            withCommand("-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false");
            switch (getPartitioner()){
                case "byteordered":
                    withClasspathResourceMapping(getConfigPrefix() + "-byteordered.yaml", "/etc/cassandra/cassandra.yaml", BindMode.READ_WRITE);
                    break;
                case "murmur":
                    if (useSSL()) {
                        withClasspathResourceMapping("cert/node.crt", "/etc/ssl/node.crt", BindMode.READ_WRITE);
                        withClasspathResourceMapping("cert/node.keystore", "/etc/ssl/node.keystore", BindMode.READ_WRITE);
                        withClasspathResourceMapping("cqlshrc", "/root/.cassandra/cqlshrc", BindMode.READ_WRITE);
                        if(enableClientAuth()) {
                            withClasspathResourceMapping("cert/node.truststore", "/etc/ssl/node.truststore", BindMode.READ_WRITE);
                            withClasspathResourceMapping(getConfigPrefix() + "-murmur-client-auth.yaml", "/etc/cassandra/cassandra.yaml", BindMode.READ_WRITE);
                        } else {
                            withClasspathResourceMapping(getConfigPrefix() + "-murmur-ssl.yaml", "/etc/cassandra/cassandra.yaml", BindMode.READ_WRITE);
                        }
                    } else {
                        withClasspathResourceMapping(getConfigPrefix() + "-murmur.yaml", "/etc/cassandra/cassandra.yaml", BindMode.READ_WRITE);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized partitioner: " + getPartitioner());
            }
        }
    }

    private static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());

        if (48 < raw.length() || raw.matches("^.*[^a-zA-Z0-9_].*$")) {
            return "strhash" + Math.abs(raw.hashCode());
        } else {
            return raw;
        }
    }

    public ModifiableConfiguration getConfiguration(final String keyspace) {
        final ModifiableConfiguration config = buildGraphConfiguration();
        config.set(KEYSPACE, cleanKeyspaceName(keyspace));
        LOGGER.debug("Set keyspace name: {}", config.get(KEYSPACE));
        config.set(PAGE_SIZE, 500);
        config.set(CONNECTION_TIMEOUT, Duration.ofSeconds(60L));
        config.set(STORAGE_BACKEND, "cql");
        config.set(STORAGE_PORT, getMappedPort(CQL_PORT));
        config.set(STORAGE_HOSTS, new String[]{getContainerIpAddress()});
        config.set(DROP_ON_CLEAR, false);
        config.set(REMOTE_MAX_REQUESTS_PER_CONNECTION, 1024);
        if (useDynamicConfig()) {
            if(useSSL()) {
                config.set(SSL_ENABLED, true);
                config.set(SSL_TRUSTSTORE_LOCATION,
                    String.join(File.separator, "target", "test-classes", "cert", "client.truststore"));
                config.set(SSL_TRUSTSTORE_PASSWORD, "client");
            }
            if (enableClientAuth()) {
                config.set(SSL_CLIENT_AUTHENTICATION_ENABLED, true);
                config.set(SSL_KEYSTORE_LOCATION,
                    String.join(File.separator, "target", "test-classes", "cert", "client.keystore"));
                config.set(SSL_KEYSTORE_STORE_PASSWORD, "client");
                config.set(SSL_KEYSTORE_KEY_PASSWORD, "client");
            }
        }
        return config;
    }

    public int getMappedCQLPort() {
        return getMappedPort(CQL_PORT);
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
            field.set(null, Collections.unmodifiableSet(StandardStoreManager.CQL.getShorthands()));

            field = StandardStoreManager.class.getDeclaredField("ALL_MANAGER_CLASSES");
            field.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, Collections.singletonMap(StandardStoreManager.CQL.getFirstStoreShorthand(), StandardStoreManager.CQL.getManagerClass()));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to set wrapper CQL store manager", e);
        }
    }
}