// Copyright 2019 JanusGraph Auts
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.util.Objects;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class JanusGraphCassandraContainer extends CassandraContainer<JanusGraphCassandraContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JanusGraphCassandraContainer.class);

    public static final String DEFAULT_VERSION = "3.11.4";
    public static final String DEFAULT_IMAGE = "cassandra";
    private boolean enableSSL;

    static {
        setWrapperStoreManager();
    }

    private static String getVersion() {
        String property = System.getProperty("cassandra.docker.version");
        if (property != null)
            return property;
        return DEFAULT_VERSION;
    }

    public static boolean isCompactStorageSupported() {
        return !getVersion().startsWith("3.") && getCassandraImage().equals(DEFAULT_IMAGE);
    }

    private static String getCassandraImage() {
        String property = System.getProperty("cassandra.docker.image");
        if (property != null)
            return property;
        return DEFAULT_IMAGE;
    }

    private static boolean isByteOrderedPartitionerEnable() {
        String property = System.getProperty("cassandra.docker.partitioner");
        return Objects.equals(property, "byteorderedpartitioner");
    }

    private String getConfigPrefix() {
        if (getVersion().startsWith("3.")) {
            return "cassandra3";
        }
        return "cassandra2";
    }

    public JanusGraphCassandraContainer() {
        this(false);
    }

    public JanusGraphCassandraContainer(boolean enableSSL) {
        super(getCassandraImage() + ":" + getVersion());
        this.enableSSL = enableSSL;
        withEnv("MAX_HEAP_SIZE", "2G");
        withEnv("HEAP_NEWSIZE", "1G");
        withCommand("-Dcassandra.config=/opt/cassandra.yaml -Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false");
        if (isByteOrderedPartitionerEnable()) {
            withClasspathResourceMapping(getConfigPrefix() + "-byteorder.yaml", "/opt/cassandra.yaml", BindMode.READ_WRITE);
        } else {
            if (this.enableSSL) {
                withClasspathResourceMapping("cert/test.crt", "/etc/ssl/test.crt", BindMode.READ_WRITE);
                withClasspathResourceMapping("cert/test.keystore", "/etc/ssl/test.keystore", BindMode.READ_WRITE);
                withClasspathResourceMapping("cqlshrc", "/root/.cassandra/cqlshrc", BindMode.READ_WRITE);
                withClasspathResourceMapping(getConfigPrefix() + "-murmur-ssl.yaml", "/opt/cassandra.yaml", BindMode.READ_WRITE);
            } else {
                withClasspathResourceMapping(getConfigPrefix() + "-murmur.yaml", "/opt/cassandra.yaml", BindMode.READ_WRITE);
            }
        }
    }

    private static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());

        if (48 < raw.length() || raw.matches("^.*[^a-zA-Z0-9_].*$")) {
            return "strhash" + String.valueOf(Math.abs(raw.hashCode()));
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
        if (enableSSL) {
            config.set(SSL_ENABLED, true);
            config.set(SSL_TRUSTSTORE_LOCATION,
                Joiner.on(File.separator).join("target", "test-classes", "cert", "test.truststore"));
            config.set(SSL_TRUSTSTORE_PASSWORD, "cassandra");
        }
        return config;
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
}