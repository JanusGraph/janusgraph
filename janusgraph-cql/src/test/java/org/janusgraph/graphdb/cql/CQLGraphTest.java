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

package org.janusgraph.graphdb.cql;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.util.backpressure.SemaphoreQueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.builder.QueryBackPressureBuilder;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BACK_PRESSURE_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BACK_PRESSURE_LIMIT;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BATCH_STATEMENT_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REMOTE_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ASSIGN_TIMESTAMP;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class CQLGraphTest extends JanusGraphTest {

    private final Logger log = LoggerFactory.getLogger(CQLGraphTest.class);

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    protected static Stream<Arguments> generateSemaphoreBackPressureConfigs() {
        return Arrays.stream(new Arguments[]{
            arguments(0, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(1, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(2, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(3, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(10, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(100, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(1000, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(1500, QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS),
            arguments(0, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
            arguments(1, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
            arguments(2, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
            arguments(3, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
            arguments(10, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
            arguments(100, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
            arguments(1000, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
            arguments(1500, QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS),
        });
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName()).getConfiguration();
    }

    @Test
    public void testHasTTL() {
        assertTrue(features.hasCellTTL());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void simpleLogTest(boolean useStringId) {
        for (int i = 0; i < 3; i++) {
            try {
                super.simpleLogTest(useStringId);
                // If the test passes, break out of the loop.
                break;
            } catch (Exception ex) {
                log.info("Attempt #{} fails", i, ex);
            }
        }
    }

    protected static Stream<Arguments> generateConsistencyConfigs() {
        return Arrays.stream(new Arguments[]{
            arguments(true, true, 20),
            arguments(true, false, 20),
            arguments(true, false, 1),
            arguments(false, true, 20),
            arguments(false, false, 20),
            arguments(false, false, 1),
        });
    }

    @Override
    @Test
    @Disabled
    public void testConsistencyEnforcement() {
        // disable original test in favour of parameterized test
    }

    @ParameterizedTest
    @MethodSource("generateConsistencyConfigs")
    public void testConsistencyEnforcement(boolean assignTimestamp, boolean atomicBatch, int batchSize) {
        clopen(option(ASSIGN_TIMESTAMP), assignTimestamp, option(ATOMIC_BATCH_MUTATE), atomicBatch, option(BATCH_STATEMENT_SIZE), batchSize);
        super.testConsistencyEnforcement();
    }

    @Override
    @Test
    @Disabled
    public void testConcurrentConsistencyEnforcement() {
        // disable original test in favour of parameterized test
    }

    @ParameterizedTest
    @MethodSource("generateConsistencyConfigs")
    public void testConcurrentConsistencyEnforcement(boolean assignTimestamp, boolean atomicBatch, int batchSize) throws Exception {
        clopen(option(ASSIGN_TIMESTAMP), assignTimestamp, option(ATOMIC_BATCH_MUTATE), atomicBatch, option(BATCH_STATEMENT_SIZE), batchSize);
        super.testConcurrentConsistencyEnforcement();
    }

    @Test
    public void testQueryLongForPropertyKey() {
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();
        finishSchema();

        /* We need to generate a string that is not simple to force the CQL
         * query to throw a ServerError because the key exceeds 64k, which is a
         * compressed form of the string.  This key ends up being 75224 bytes. */
        String testName = RandomStringUtils.random(100000, 0, 0, true, true, null, new Random(0));

        GraphTraversalSource g = graph.traversal();
        JanusGraphException ex = assertThrows(JanusGraphException.class,
                () -> g.V().has("name", testName).hasNext());
        assertEquals(-1, ExceptionUtils.indexOfType(ex, TemporaryBackendException.class),
                "Query should not produce a TemporaryBackendException");
        assertNotEquals(-1, ExceptionUtils.indexOfType(ex, PermanentBackendException.class),
                "Query should produce a PermanentBackendException");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10, 100, 1000})
    public void fetchElementsUsingDifferentPageSize(int pageSize) {
        clopen(option(USE_MULTIQUERY), true, option(PAGE_SIZE), pageSize);
        assertSingleTxAdditionAndCount(10, pageSize * 3);
    }

    @ParameterizedTest
    @MethodSource("generateSemaphoreBackPressureConfigs")
    public void testDifferentBackpressureLimitIsApplicable(int backPressureLimit, String backPressureClass) {
        clopen(option(BACK_PRESSURE_LIMIT), backPressureLimit,
            option(BACK_PRESSURE_CLASS), backPressureClass,
            option(MAX_REQUESTS_PER_CONNECTION), Math.max(MAX_REQUESTS_PER_CONNECTION.getDefaultValue(), backPressureLimit),
            option(USE_MULTIQUERY), true);
        assertSingleTxAdditionAndCount(10, 20);
    }

    @Test
    public void testBackpressureIsDisabled() {
        clopen(option(BACK_PRESSURE_CLASS), QueryBackPressureBuilder.PASS_ALL_QUERY_BACK_PRESSURE_CLASS,
            option(USE_MULTIQUERY), true, option(MAX_REQUESTS_PER_CONNECTION), 1024, option(LOCAL_MAX_CONNECTIONS_PER_HOST), 20,
            option(REMOTE_MAX_CONNECTIONS_PER_HOST), 1);
        assertSingleTxAdditionAndCount(10, 20);
    }

    @Test
    public void testCustomBackPressureClassIsSet() {
        CustomQueryBackPressure.acquireIsUsed = false;
        CustomQueryBackPressure.releaseIsUsed = false;
        clopen(option(BACK_PRESSURE_CLASS), CustomQueryBackPressure.class.getName(), option(USE_MULTIQUERY), true);
        assertSingleTxAdditionAndCount(10, 20);
        assertTrue(CustomQueryBackPressure.acquireIsUsed);
        assertTrue(CustomQueryBackPressure.releaseIsUsed);
    }

    private void assertSingleTxAdditionAndCount(int indexedVerticesCount, int adjacentVerticesCount){
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.makeEdgeLabel("testEdge").multiplicity(Multiplicity.SIMPLE).make();
        finishSchema();

        GraphTraversalSource g = graph.traversal();
        for(int i=0; i < indexedVerticesCount; i++){
            Vertex indexedVertex = g.addV().property("name", "testName").next();
            for(int j=0; j<adjacentVerticesCount; j++){
                Vertex adjacentVertex = g.addV().next();
                indexedVertex.addEdge("testEdge", adjacentVertex);
            }
        }

        g.tx().commit();

        int idsCount = g.V().has("name", "testName").out("testEdge").id().toList().size();

        assertEquals(indexedVerticesCount * adjacentVerticesCount, idsCount);
    }

    public static class CustomQueryBackPressure extends SemaphoreQueryBackPressure{

        public static volatile boolean acquireIsUsed;
        public static volatile boolean releaseIsUsed;

        public CustomQueryBackPressure(Configuration configuration, Integer backPressureLimit) {
            super(backPressureLimit);
            // configuration ignored
        }

        @Override
        public void acquireBeforeQuery() {
            acquireIsUsed = true;
            super.acquireBeforeQuery();
        }

        @Override
        public void releaseAfterQuery(){
            releaseIsUsed = true;
            super.releaseAfterQuery();
        }
    }
}
