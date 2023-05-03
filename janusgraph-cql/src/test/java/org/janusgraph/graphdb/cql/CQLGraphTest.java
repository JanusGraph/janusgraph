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

import io.github.artsok.RepeatedIfExceptionsTest;
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
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
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
    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName()).getConfiguration();
    }

    @Test
    public void testHasTTL() {
        assertTrue(features.hasCellTTL());
    }

    @RepeatedIfExceptionsTest(repeats = 3)
    @Override
    public void simpleLogTest() throws InterruptedException{
        super.simpleLogTest();
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
    @ValueSource(ints = {0, 1, 2, 3, 10, 100, 1000})
    public void testDifferentBackpressureLimitIsApplicable(int backPressureLimit) {
        clopen(option(BACK_PRESSURE_LIMIT), backPressureLimit, option(USE_MULTIQUERY), true);
        assertSingleTxAdditionAndCount(10, 20);
    }

    @Test
    public void testBackpressureIsDisabled() {
        clopen(option(BACK_PRESSURE_LIMIT), -1, option(USE_MULTIQUERY), true,
            option(MAX_REQUESTS_PER_CONNECTION), 1024, option(LOCAL_MAX_CONNECTIONS_PER_HOST), 20, option(REMOTE_MAX_CONNECTIONS_PER_HOST), 1);
        assertSingleTxAdditionAndCount(10, 20);
    }

    @Test
    public void testInvalidBackpressureLimitIsNotApplicable() {
        assertThrows(Throwable.class, () -> clopen(option(BACK_PRESSURE_LIMIT), -2, option(USE_MULTIQUERY), true));
        assertThrows(Throwable.class, () -> clopen(option(BACK_PRESSURE_LIMIT), -100, option(USE_MULTIQUERY), true));
        assertThrows(Throwable.class, () -> clopen(option(BACK_PRESSURE_LIMIT), Integer.MIN_VALUE, option(USE_MULTIQUERY), true));
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
}
