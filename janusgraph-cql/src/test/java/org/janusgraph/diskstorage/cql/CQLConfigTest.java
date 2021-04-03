// Copyright 2021 JanusGraph Authors
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

import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.stream.Stream;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_SCHEMA_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_TOKEN_MAP_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.PARTITIONER_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_ERROR_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_QUERY_LENGTH;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_VALUES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_VALUE_LENGTH;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SHOW_STACK_TRACES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SHOW_VALUES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SLOW_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SLOW_THRESHOLD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SUCCESS_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_TRACKER_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class CQLConfigTest {

    private StandardJanusGraph graph;

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    @AfterEach
    public void tearDown() {
        if (graph != null && graph.isOpen()) {
            graph.close();
        }
    }

    private WriteConfiguration getConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName()).getConfiguration();
    }

    public static Stream<Arguments> getMetadataConfigs() {
        return Stream.of(
            Arguments.of(null, true, true),
            Arguments.of(null, true, false),
            Arguments.of(null, false, true),
            Arguments.of(null, false, false),
            Arguments.of("Murmur3Partitioner", true, true),
            Arguments.of("Murmur3Partitioner", true, false),
            Arguments.of("Murmur3Partitioner", false, true),
            Arguments.of("Murmur3Partitioner", false, false)
        );
    }

    @Test
    public void testTitanGraphBackwardCompatibility() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(KEYSPACE), "titan");
        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "x.x.x");

        assertNull(wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION),
            GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION.getDatatype()));

        assertFalse(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
            wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS),
                GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())));

        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "1.0.0");
        assertTrue(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
            wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS),
                GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())));

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
    }

    @Test
    public void testStorageVersionSet() {
        WriteConfiguration wc = getConfiguration();
        assertNull(wc.get(ConfigElement.getPath(INITIAL_STORAGE_VERSION), INITIAL_STORAGE_VERSION.getDatatype()));
        wc.set(ConfigElement.getPath(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION);
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        JanusGraphManagement mgmt = graph.openManagement();
        assertEquals(JanusGraphConstants.STORAGE_VERSION, (mgmt.get("graph.storage-version")));
        mgmt.rollback();
    }

    private String getCurrentPartitionerName() {
        switch (System.getProperty("cassandra.docker.partitioner")) {
            case "byteordered":
                return "ByteOrderedPartitioner";
            case "murmur":
            default:
                return "Murmur3Partitioner";
        }
    }

    @ParameterizedTest
    @MethodSource("getMetadataConfigs")
    public void testMetaDataGraphConfig(String partitionerName, boolean schemaEnabled, boolean tokenMapEnabled) {
        WriteConfiguration wc = getConfiguration();
        String currentPartitionerName = getCurrentPartitionerName();
        if (partitionerName != null) wc.set(ConfigElement.getPath(PARTITIONER_NAME), partitionerName);
        assertNull(wc.get(ConfigElement.getPath(METADATA_SCHEMA_ENABLED), METADATA_SCHEMA_ENABLED.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(METADATA_TOKEN_MAP_ENABLED), METADATA_TOKEN_MAP_ENABLED.getDatatype()));
        wc.set(ConfigElement.getPath(METADATA_SCHEMA_ENABLED), schemaEnabled);
        wc.set(ConfigElement.getPath(METADATA_TOKEN_MAP_ENABLED), tokenMapEnabled);

        if (tokenMapEnabled && partitionerName != null && !partitionerName.equals(currentPartitionerName) // not matching
            || !tokenMapEnabled && partitionerName == null // not provided and cannot be retrieved
        ) {
            assertThrows(IllegalArgumentException.class, () -> JanusGraphFactory.open(wc));
        } else {
            JanusGraphFactory.open(wc);
        }
    }


    @Test
    public void testGraphConfigUsedByThreadBoundTx() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(WRITE_CONSISTENCY), "LOCAL_QUORUM");

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.getCurrentThreadTx();
        assertEquals("ALL",
            tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                .get(READ_CONSISTENCY));
        assertEquals("LOCAL_QUORUM",
            tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                .get(WRITE_CONSISTENCY));
    }

    @Test
    public void testGraphConfigUsedByTx() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(READ_CONSISTENCY), "TWO");
        wc.set(ConfigElement.getPath(WRITE_CONSISTENCY), "THREE");

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        assertEquals("TWO",
            tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                .get(READ_CONSISTENCY));
        assertEquals("THREE",
            tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                .get(WRITE_CONSISTENCY));
        tx.rollback();
    }

    @Test
    public void testCustomConfigUsedByTx() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(WRITE_CONSISTENCY), "ALL");

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction()
            .customOption(ConfigElement.getPath(READ_CONSISTENCY), "ONE")
            .customOption(ConfigElement.getPath(WRITE_CONSISTENCY), "TWO").start();

        assertEquals("ONE",
            tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                .get(READ_CONSISTENCY));
        assertEquals("TWO",
            tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                .get(WRITE_CONSISTENCY));
        tx.rollback();
    }

    @Test
    public void testRequestLoggerConfigurationSet() {
        WriteConfiguration wc = getConfiguration();
        assertNull(wc.get(ConfigElement.getPath(REQUEST_TRACKER_CLASS), REQUEST_TRACKER_CLASS.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_SUCCESS_ENABLED), REQUEST_LOGGER_SUCCESS_ENABLED.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_SLOW_THRESHOLD), REQUEST_LOGGER_SLOW_THRESHOLD.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_SLOW_ENABLED), REQUEST_LOGGER_SLOW_ENABLED.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_ERROR_ENABLED), REQUEST_LOGGER_ERROR_ENABLED.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_MAX_QUERY_LENGTH), REQUEST_LOGGER_MAX_QUERY_LENGTH.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_SHOW_VALUES), REQUEST_LOGGER_SHOW_VALUES.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_MAX_VALUE_LENGTH), REQUEST_LOGGER_MAX_VALUE_LENGTH.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_MAX_VALUES), REQUEST_LOGGER_MAX_VALUES.getDatatype()));
        assertNull(wc.get(ConfigElement.getPath(REQUEST_LOGGER_SHOW_STACK_TRACES), REQUEST_LOGGER_SHOW_STACK_TRACES.getDatatype()));

        wc.set(ConfigElement.getPath(REQUEST_TRACKER_CLASS), RequestLogger.class.getSimpleName());
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_SUCCESS_ENABLED), true);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_SLOW_THRESHOLD), 1L);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_SLOW_ENABLED), true);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_ERROR_ENABLED), true);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_MAX_QUERY_LENGTH), 100000);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_SHOW_VALUES), true);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_MAX_VALUE_LENGTH), 100000);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_MAX_VALUES), 100000);
        wc.set(ConfigElement.getPath(REQUEST_LOGGER_SHOW_STACK_TRACES), true);

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        assertDoesNotThrow(() -> {
            graph.traversal().V().hasNext();
            graph.tx().rollback();
        });
    }

}
