// Copyright 2026 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ExecutorServiceConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_MAX_INLINE_ROWS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.STRING_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The result of a slice query which is one page of at most {@code storage.cql.executor-service.max-inline-rows} rows is
 * turned into entries on the driver's I/O thread; a larger result, and one of several pages, on the executor service.
 */
@Testcontainers
public class CQLReadDeserializationTest {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    private static final int PROPERTIES = 5;
    private static final int EDGES = 150;

    private JanusGraph graph;

    //The executor service of the CQL store manager, counting the tasks it is handed
    public static final class CountingExecutorService extends ThreadPoolExecutor {

        static final AtomicLong tasks = new AtomicLong();

        public CountingExecutorService(ExecutorServiceConfiguration configuration) {
            super(2, 2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), runnable -> {
                final Thread thread = new Thread(runnable, "counting-cql-executor");
                thread.setDaemon(true);
                return thread;
            });
        }

        @Override
        public void execute(Runnable command) {
            tasks.incrementAndGet();
            super.execute(command);
        }
    }

    @AfterEach
    public void tearDown() {
        if (graph != null && graph.isOpen()) {
            graph.close();
        }
    }

    private void open(String keyspace, Integer maxInlineRows, Integer driverPageSize) {
        final WriteConfiguration config = cqlContainer.getConfiguration(keyspace).getConfiguration();
        config.set(ConfigElement.getPath(EXECUTOR_SERVICE_CLASS), CountingExecutorService.class.getName());
        if (maxInlineRows != null) {
            config.set(ConfigElement.getPath(EXECUTOR_SERVICE_MAX_INLINE_ROWS), maxInlineRows);
        }
        if (driverPageSize != null) {
            config.set(ConfigElement.getPath(STRING_CONFIGURATION),
                "datastax-java-driver { basic.request.page-size = " + driverPageSize + " }");
        }
        graph = JanusGraphFactory.open(config);

        //The vertices are found through an index, a read of one row, rather than a scan of every vertex
        final JanusGraphManagement mgmt = graph.openManagement();
        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("byName", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.commit();

        final GraphTraversalSource g = graph.traversal();
        final Vertex alice = g.addV("person").property("name", "alice").next();
        for (int i = 0; i < PROPERTIES; i++) {
            alice.property("property" + i, "value" + i);
        }
        final Vertex hub = g.addV("person").property("name", "hub").next();
        for (int i = 0; i < EDGES; i++) {
            hub.addEdge("knows", g.addV("person").property("name", "friend" + i).next());
        }
        g.tx().commit();
    }

    //Runs the read twice, so that the schema it needs is cached, and returns the executor's tasks of the second run
    private long tasksOf(Runnable read) {
        read.run();
        graph.tx().rollback();
        final long before = CountingExecutorService.tasks.get();
        read.run();
        graph.tx().rollback();
        return CountingExecutorService.tasks.get() - before;
    }

    private void readAlicesProperties() {
        final Map<Object, Object> properties = graph.traversal().V().has("name", "alice").valueMap().next();
        assertEquals(PROPERTIES + 1, properties.size());
    }

    private void readHubsEdges() {
        assertEquals(EDGES, graph.traversal().V().has("name", "hub").outE("knows").count().next());
    }

    private void readAlicesEdges() {
        assertEquals(0, graph.traversal().V().has("name", "alice").outE("knows").count().next());
    }

    private void findAlice() {
        assertNotNull(graph.traversal().V().has("name", "alice").id().next());
    }

    @Test
    public void shouldDeserializeAResultOfOnePageOfFewRowsOnTheDriversThread() {
        open("inline_small", null, null);
        assertEquals(0, tasksOf(this::readAlicesProperties));
        assertEquals(0, tasksOf(this::readAlicesEdges));
    }

    @Test
    public void shouldDeserializeAResultOfMoreRowsThanTheLimitOnTheExecutor() {
        open("inline_large", null, null);
        assertTrue(tasksOf(this::readHubsEdges) > 0);
        assertEquals(0, tasksOf(this::readAlicesProperties));
    }

    @Test
    public void shouldDeserializeAResultOfSeveralPagesOnTheExecutor() {
        open("inline_pages", null, 3);
        assertTrue(tasksOf(this::readAlicesProperties) > 0);
    }

    //With the option at 0 the index lookup which finds alice, a row, uses the executor, but her edges, of which there
    //are none, cost no task beyond finding her
    @Test
    public void shouldDeserializeEveryResultWithRowsOnTheExecutorWithoutInlineRows() {
        open("inline_off", 0, null);
        assertTrue(tasksOf(this::readAlicesProperties) > 0);
        final long findingAlice = tasksOf(this::findAlice);
        assertTrue(findingAlice > 0);
        assertEquals(findingAlice, tasksOf(this::readAlicesEdges));
    }
}
