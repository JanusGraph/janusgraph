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

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.IDEMPOTENT_WRITES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_TRACKER_CLASS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ASSIGN_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The driver resends a request, to the same node or another, after a closed connection or an error response, and runs
 * speculative executions, only for statements marked idempotent. JanusGraph's reads always are, as the query builder
 * marks a SELECT idempotent. Its writes, which it sends in batches, are while it binds their timestamps, as a write sent
 * again then writes the same cells with the same timestamp, unless storage.cql.idempotent-writes is off.
 */
@Testcontainers
public class CQLStatementIdempotenceTest {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    private JanusGraph graph;

    //Every request the driver completed
    public static final class RecordingRequestTracker implements RequestTracker {

        static final Queue<Request> requests = new ConcurrentLinkedQueue<>();

        public RecordingRequestTracker(DriverContext context) {
        }

        @Override
        public void onSuccess(Request request, long latencyNanos, DriverExecutionProfile executionProfile, Node node,
                              String requestLogPrefix) {
            requests.add(request);
        }

        @Override
        public void close() {
        }
    }

    @AfterEach
    public void tearDown() {
        if (graph != null && graph.isOpen()) {
            graph.close();
        }
    }

    private void open(String keyspace, boolean assignTimestamp, boolean idempotentWrites, boolean atomicBatches) {
        final WriteConfiguration config = cqlContainer.getConfiguration(keyspace).getConfiguration();
        config.set(ConfigElement.getPath(REQUEST_TRACKER_CLASS), RecordingRequestTracker.class.getName());
        config.set(ConfigElement.getPath(ASSIGN_TIMESTAMP), assignTimestamp);
        config.set(ConfigElement.getPath(IDEMPOTENT_WRITES), idempotentWrites);
        config.set(ConfigElement.getPath(ATOMIC_BATCH_MUTATE), atomicBatches);
        graph = JanusGraphFactory.open(config);
    }

    //Writes, also with a TTL, updates and deletes, then reads a vertex's properties, its edges and every vertex
    private void readAndWrite() {
        final JanusGraphManagement mgmt = graph.openManagement();
        final VertexLabel event = mgmt.makeVertexLabel("event").setStatic().make();
        mgmt.setTTL(event, Duration.ofHours(1));
        mgmt.commit();

        final GraphTraversalSource g = graph.traversal();
        final Vertex alice = g.addV("person").property("name", "alice").property("age", 30).next();
        final Vertex bob = g.addV("person").property("name", "bob").property("age", 40).next();
        g.V(alice).addE("knows").to(bob).iterate();
        g.addV("event").property("name", "launch").iterate();
        g.tx().commit();

        g.V(alice).property("age", 31).iterate();
        g.V(bob).drop().iterate();
        g.tx().commit();

        assertEquals(31, (int) g.V(alice).<Integer>values("age").next());
        assertEquals(2, g.V(alice).valueMap("name", "age").next().size());
        assertEquals(0, g.V(alice).outE("knows").count().next());
        assertEquals(2, g.V().count().next());
        g.tx().rollback();
    }

    private static String query(BoundStatement statement) {
        return statement.getPreparedStatement().getQuery().trim().toUpperCase();
    }

    //A full scan filters by column, and a scan of a token range or an ordered one selects by token
    private static boolean isScan(String query) {
        return query.contains("ALLOW FILTERING") || query.contains("TOKEN(");
    }

    //The driver tells the tracker of a request only after it has completed the request's result, which the traversal
    //may have taken by then. The scan which ends readAndWrite is its last request, so the checks wait for it to be told
    private static void awaitScan() throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (RecordingRequestTracker.requests.stream().noneMatch(request -> request instanceof BoundStatement
                && isScan(query((BoundStatement) request))) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    //Checks the idempotence of every read and write the driver completed, and that there were reads, scans, insertions,
    //insertions with a TTL and deletions to check
    private static void assertIdempotence(Boolean writes) throws InterruptedException {
        awaitScan();
        final List<String> reads = new ArrayList<>();
        final List<String> writesSent = new ArrayList<>();
        for (Request request : RecordingRequestTracker.requests) {
            if (request instanceof BoundStatement) {
                final BoundStatement statement = (BoundStatement) request;
                final String query = query(statement);
                if (query.startsWith("SELECT")) {
                    assertEquals(Boolean.TRUE, statement.isIdempotent(), query);
                    reads.add(query);
                } else {
                    assertEquals(writes, statement.isIdempotent(), query);
                    writesSent.add(query);
                }
            } else if (request instanceof BatchStatement) {
                final BatchStatement batch = (BatchStatement) request;
                assertEquals(writes, batch.isIdempotent(), batch.getBatchType().toString());
                //The driver reads only the batch's flag, but a statement executed on its own would be read for its own
                for (BatchableStatement<?> statement : batch) {
                    final String query = query((BoundStatement) statement);
                    assertEquals(writes, statement.isIdempotent(), query);
                    writesSent.add(query);
                }
            }
        }
        assertTrue(reads.stream().anyMatch(query -> query.contains(" IN ")), "no read of several columns: " + reads);
        assertTrue(reads.stream().anyMatch(CQLStatementIdempotenceTest::isScan), "no scan: " + reads);
        assertTrue(writesSent.stream().anyMatch(query -> query.startsWith("INSERT")), "no insertion: " + writesSent);
        assertTrue(writesSent.stream().anyMatch(query -> query.contains(" TTL ")), "no insertion with a TTL: " + writesSent);
        assertTrue(writesSent.stream().anyMatch(query -> query.startsWith("DELETE")), "no deletion: " + writesSent);
    }

    @Test
    public void shouldMarkReadsAndWritesWithJanusGraphsTimestampsIdempotent() throws InterruptedException {
        open("idempotent_unlogged", true, true, false);
        RecordingRequestTracker.requests.clear();
        readAndWrite();
        assertIdempotence(Boolean.TRUE);
    }

    @Test
    public void shouldMarkLoggedBatchesWithJanusGraphsTimestampsIdempotent() throws InterruptedException {
        open("idempotent_logged", true, true, true);
        RecordingRequestTracker.requests.clear();
        readAndWrite();
        assertIdempotence(Boolean.TRUE);
    }

    //Without JanusGraph's timestamps a write sent again may get a newer one, so writes keep the driver's default
    @Test
    public void shouldLeaveWritesWithoutJanusGraphsTimestampsToTheDriversDefault() throws InterruptedException {
        open("idempotent_driver_timestamps", false, true, false);
        RecordingRequestTracker.requests.clear();
        readAndWrite();
        assertIdempotence(null);
    }

    //With storage.cql.idempotent-writes off, writes keep the driver's default even with JanusGraph's timestamps
    @Test
    public void shouldLeaveWritesToTheDriversDefaultWithIdempotentWritesOff() throws InterruptedException {
        open("idempotent_writes_off_unlogged", true, false, false);
        RecordingRequestTracker.requests.clear();
        readAndWrite();
        assertIdempotence(null);
    }

    @Test
    public void shouldLeaveLoggedBatchesToTheDriversDefaultWithIdempotentWritesOff() throws InterruptedException {
        open("idempotent_writes_off_logged", true, false, true);
        RecordingRequestTracker.requests.clear();
        readAndWrite();
        assertIdempotence(null);
    }
}
