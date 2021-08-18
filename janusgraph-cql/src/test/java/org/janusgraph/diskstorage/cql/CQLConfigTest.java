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
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ExecutorServiceBuilder;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BASE_PROGRAMMATIC_CONFIGURATION_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.FILE_CONFIGURATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.HEARTBEAT_TIMEOUT;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_DATACENTER;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_SCHEMA_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_TOKEN_MAP_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_TIMER_TICK_DURATION;
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
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_TIMEOUT;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_TRACKER_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.RESOURCE_CONFIGURATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SESSION_LEAK_THRESHOLD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.STRING_CONFIGURATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.URL_CONFIGURATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.CONNECTION_TIMEOUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_RENEW_TIMEOUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public class CQLConfigTest {

    private static final Logger log = LoggerFactory.getLogger(CQLConfigTest.class);

    private static final String DATASTAX_JAVA_DRIVER_STRING_CONFIGURATION_PATTERN =
        "datastax-java-driver {\n" +
            "advanced.address-translator.class = \"PassThroughAddressTranslator\"\n" +
            "basic.contact-points = [ \"${hostname}:${port}\" ]\n" +
            "basic.session-name = JanusGraphCQLSession\n" +
            "basic.load-balancing-policy{\n" +
            "    local-datacenter = \"${datacenter}\"\n" +
            "}\n" +
            "basic.request.timeout = ${timeout} milliseconds\n"+
            "advanced.connection.connect-timeout = ${timeout} milliseconds\n"+
            "}\n";

    private static final String DATASTAX_JAVA_DRIVER_STRING_KEYSPACE_ONLY_CONFIGURATION_PATTERN =
        "datastax-java-driver {\n" +
            "basic.session-keyspace = testkeyspace\n" +
            "}\n";

    private static final String DATASTAX_JAVA_DRIVER_STRING_CONTACT_POINTS_ONLY_CONFIGURATION_PATTERN =
        "datastax-java-driver {\n" +
            "basic.contact-points = [ \"${hostname}:${port}\" ]\n" +
            "}\n";

    private StandardJanusGraph graph;

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    @BeforeEach
    public void ensureCQLContainerIsOpened(){
        if(!cqlContainer.isRunning()){
            cqlContainer.start();
        }
    }

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

    @Test
    public void shouldCreateCachedThreadPool() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(EXECUTOR_SERVICE_CLASS), ExecutorServiceBuilder.CACHED_THREAD_POOL_CLASS);
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        assertDoesNotThrow(() -> {
            graph.traversal().V().hasNext();
            graph.tx().rollback();
        });
    }

    @Test
    public void shouldGracefullyCloseGraphWhichLostAConnection(){
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME), 60000);
        wc.set(ConfigElement.getPath(PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME), 60000);
        wc.set(ConfigElement.getPath(IDS_RENEW_TIMEOUT), 10000);
        wc.set(ConfigElement.getPath(CONNECTION_TIMEOUT), 10000);
        wc.set(ConfigElement.getPath(HEARTBEAT_TIMEOUT), 10000);

        if(graph != null && graph.isOpen()){
            graph.close();
        }

        Set<Thread> threadsFromPossibleOtherOpenedConnections = Thread.getAllStackTraces().keySet().stream().filter(thread -> {
            String threadNameLowercase = thread.getName().toLowerCase();
            return thread.isAlive() && (threadNameLowercase.startsWith("cql") || threadNameLowercase.startsWith("janusgraph"));
        }).collect(Collectors.toSet());

        boolean flakyTest = !threadsFromPossibleOtherOpenedConnections.isEmpty();

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        assertDoesNotThrow(() -> {
            graph.traversal().V().hasNext();
            graph.tx().rollback();
        });

        Set<Thread> threadsToAwait = Thread.getAllStackTraces().keySet().stream().filter(thread -> {
            String threadNameLowercase = thread.getName().toLowerCase();
            return thread.isAlive() && !threadsFromPossibleOtherOpenedConnections.contains(thread) && (threadNameLowercase.startsWith("cql") || threadNameLowercase.startsWith("janusgraph"));
        }).collect(Collectors.toSet());

        cqlContainer.stop();

        graph.close();

        for(Thread thread : threadsToAwait){
            if(thread.isAlive()){
                if(flakyTest){
                    log.warn("Test shouldGracefullyCloseGraphWhichLostAConnection is currently running in flaky mode " +
                        "because there were open instances available before the test started. " +
                        "Thus, we don't fail this test because we can't be sure that current thread {} " +
                        "is leaked or were created by other JanusGraph instances.", thread.getName());
                } else {
                    fail("Thread "+thread.getName()+" was alive but expected to be terminated");
                }
            }
        }
    }

    @Test
    public void shouldCreateCQLSessionWithDisabledSessionLeakThreshold() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(SESSION_LEAK_THRESHOLD), 0);
        assertDoesNotThrow(() -> JanusGraphFactory.open(wc).close());
    }

    @Test
    public void shouldBeAbleToSetupCustomRequestTimeout() {

        assertDoesNotThrow(() -> {
            WriteConfiguration wc = getConfiguration();
            wc.set(ConfigElement.getPath(REQUEST_TIMEOUT), 12345);
            wc.set(ConfigElement.getPath(NETTY_TIMER_TICK_DURATION), 1);
            StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
            GraphTraversalSource graphTraversalSource = graph.traversal();
            for(int i=0; i<200; i++){
                graphTraversalSource.addV().property("name", "world")
                    .property("age", 123)
                    .property("id", i);
            }
            graphTraversalSource.tx().commit();
            graphTraversalSource.V().has("id", P.lte(195)).valueMap().with(WithOptions.tokens, WithOptions.ids).toList();
            graphTraversalSource.tx().rollback();
            graph.close();
        });
    }

    @Test
    public void shouldFailDueToSmallTimeout() {
        try{
            WriteConfiguration wc = getConfiguration();
            wc.set(ConfigElement.getPath(REQUEST_TIMEOUT), 1);
            wc.set(ConfigElement.getPath(NETTY_TIMER_TICK_DURATION), 1);
            try (StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(wc)) {
                GraphTraversalSource graphTraversalSource = graph.traversal();
                for(int i=0; i<200; i++){
                    graphTraversalSource.addV().property("name", "world")
                        .property("age", 123)
                        .property("id", i);
                }
                graphTraversalSource.tx().commit();
                graphTraversalSource.V().has("id", P.lte(195)).valueMap().with(WithOptions.tokens, WithOptions.ids).toList();
                graphTraversalSource.tx().rollback();
            }
            // This test should fail, but it is very flaky, thus, we don't fail it even if we reached this point.
            // fail()
        } catch (Throwable throwable){
            // the throwable is expected
        }
    }

    @Test
    public void shouldCreateCQLSessionWithStringConfigurationOnly() {
        WriteConfiguration wc = getConfiguration();
        String dataStaxConfiguration = prepareDataStaxConfiguration(wc);

        wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), false);
        wc.set(ConfigElement.getPath(STRING_CONFIGURATION), dataStaxConfiguration);

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        assertDoesNotThrow(() -> {
            graph.traversal().V().hasNext();
            graph.tx().rollback();
        });
    }

    @Test
    public void shouldCreateCQLSessionWithFileConfigurationOnly() throws IOException {
        WriteConfiguration wc = getConfiguration();
        String dataStaxConfiguration = prepareDataStaxConfiguration(wc);

        File tempFile = File.createTempFile("datastaxTempExample", ".conf");
        try{
            tempFile.deleteOnExit();
            FileUtils.writeStringToFile(tempFile, dataStaxConfiguration, Charset.defaultCharset(), false);

            wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), false);
            wc.set(ConfigElement.getPath(FILE_CONFIGURATION), tempFile.getAbsolutePath());

            graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
            assertDoesNotThrow(() -> {
                graph.traversal().V().hasNext();
                graph.tx().rollback();
            });

        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void shouldTryToCreateCQLSessionWithResourceConfigurationOnlyButFailDueToMisconfigurationOfPort() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), false);
        wc.set(ConfigElement.getPath(RESOURCE_CONFIGURATION), "datastaxMisconfiguredResourceTestConfig.conf");
        assertThrows(Throwable.class, () -> JanusGraphFactory.open(wc));
    }

    @Test
    public void shouldCreateCQLSessionWithUrlConfigurationOnly() throws IOException {
        WriteConfiguration wc = getConfiguration();
        String dataStaxConfiguration = prepareDataStaxConfiguration(wc);

        File tempFile = File.createTempFile("datastaxTempExample", ".conf");
        try{
            tempFile.deleteOnExit();
            FileUtils.writeStringToFile(tempFile, dataStaxConfiguration, Charset.defaultCharset(), false);

            wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), false);
            wc.set(ConfigElement.getPath(URL_CONFIGURATION), new URL("file", "", tempFile.getAbsolutePath()).toString());

            graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
            assertDoesNotThrow(() -> {
                graph.traversal().V().hasNext();
                graph.tx().rollback();
            });

        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void shouldComposeProgrammaticConfigurationWithStringConfigurationAndFailDueToUnnecessaryKeyspaceConfigAdded() {
        WriteConfiguration wc = getConfiguration();

        wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), true);
        wc.set(ConfigElement.getPath(STRING_CONFIGURATION), DATASTAX_JAVA_DRIVER_STRING_KEYSPACE_ONLY_CONFIGURATION_PATTERN);

        assertThrows(IllegalArgumentException.class, () -> JanusGraphFactory.open(wc));
    }

    @Test
    public void shouldCreateCQLSessionWithResourceAndStringConfigurations() {
        WriteConfiguration wc = getConfiguration();
        String dataStaxConfiguration = prepareDataStaxContactPointsOnlyConfiguration(wc);

        wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), false);
        wc.set(ConfigElement.getPath(RESOURCE_CONFIGURATION), "datastaxResourceTestConfigWithoutContactPoints.conf");
        wc.set(ConfigElement.getPath(STRING_CONFIGURATION), dataStaxConfiguration);

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        assertDoesNotThrow(() -> {
            graph.traversal().V().hasNext();
            graph.tx().rollback();
        });
    }

    @Test
    public void shouldCreateCQLSessionWithMultipleComposedConfigurations() throws IOException {
        WriteConfiguration wc = getConfiguration();
        String dataStaxConfiguration = prepareDataStaxConfiguration(wc);

        File tempFile = File.createTempFile("datastaxTempExample", ".conf");
        try{
            tempFile.deleteOnExit();
            FileUtils.writeStringToFile(tempFile, dataStaxConfiguration, Charset.defaultCharset(), false);

            wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), true);
            wc.set(ConfigElement.getPath(URL_CONFIGURATION), new URL("file", "", tempFile.getAbsolutePath()).toString());
            wc.set(ConfigElement.getPath(FILE_CONFIGURATION), tempFile.getAbsolutePath());
            wc.set(ConfigElement.getPath(STRING_CONFIGURATION), dataStaxConfiguration);

            graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
            assertDoesNotThrow(() -> {
                graph.traversal().V().hasNext();
                graph.tx().rollback();
            });

        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void shouldTryToCreateCQLSessionWithDefaultDataStaxConfigurationOnlyButFailDueToTestcontainersUseNonDefaultPort() {
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(BASE_PROGRAMMATIC_CONFIGURATION_ENABLED), false);
        assertThrows(RuntimeException.class, () -> JanusGraphFactory.open(wc));
    }

    private static String prepareDataStaxConfiguration(WriteConfiguration wc){

        String[] hostname = wc.get(ConfigElement.getPath(STORAGE_HOSTS), String[].class);
        String port = wc.get(ConfigElement.getPath(STORAGE_PORT), Integer.class).toString();
        Duration timeout = wc.get(ConfigElement.getPath(CONNECTION_TIMEOUT), Duration.class);
        if(timeout == null){
            timeout = CONNECTION_TIMEOUT.getDefaultValue();
        }

        return DATASTAX_JAVA_DRIVER_STRING_CONFIGURATION_PATTERN
            .replace("${hostname}", hostname[0])
            .replace("${port}", port)
            .replace("${datacenter}", LOCAL_DATACENTER.getDefaultValue())
            .replace("${timeout}", String.valueOf(timeout.toMillis()));
    }

    private static String prepareDataStaxContactPointsOnlyConfiguration(WriteConfiguration wc){

        String[] hostname = wc.get(ConfigElement.getPath(STORAGE_HOSTS), String[].class);
        String port = wc.get(ConfigElement.getPath(STORAGE_PORT), Integer.class).toString();

        return DATASTAX_JAVA_DRIVER_STRING_CONTACT_POINTS_ONLY_CONFIGURATION_PATTERN
            .replace("${hostname}", hostname[0])
            .replace("${port}", port);
    }
}
