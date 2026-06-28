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

package org.janusgraph.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.MixedIndexUpdateApplier;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end (minus containers) convergence test: the real {@link DebeziumCassandraJsonDecoder} and
 * {@link MixedIndexUpdateApplier} are driven through {@link CdcIndexUpdateWorker} via a {@code MockConsumer} fed
 * Debezium-format records built from real JanusGraph edgestore bytes. The graph is cdc-only, so the worker is the sole
 * writer of the Lucene index; the assertions confirm the index converges to the current graph state for vertex and
 * edge add/update/remove, and that a stale/out-of-order event still converges to current state.
 */
public class CdcWorkerConvergenceTest {

    private static final String BACKING = "search";
    private static final String TOPIC = "cassandra.janusgraph.edgestore";

    @TempDir
    Path tempDir;

    private final ObjectMapper mapper = new ObjectMapper();
    private JanusGraph graph;
    private IDManager idManager;
    private DebeziumCassandraJsonDecoder decoder;
    private MixedIndexUpdateApplier applier;
    private CdcWorkerConfiguration config;

    @BeforeEach
    public void setUp() throws InterruptedException {
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(STORAGE_BACKEND, "inmemory");
        cfg.set(INDEX_BACKEND, "lucene", BACKING);
        cfg.set(INDEX_DIRECTORY, tempDir.resolve("lucene").toString(), BACKING);
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, BACKING);
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS, false, BACKING); // cdc-only
        graph = JanusGraphFactory.open(cfg.getConfiguration());

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey since = mgmt.makePropertyKey("since").dataType(String.class).make();
        mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex(BACKING);
        mgmt.buildIndex("esearch", Edge.class).addKey(since).buildMixedIndex(BACKING);
        mgmt.commit();
        org.janusgraph.graphdb.database.management.ManagementSystem
            .awaitGraphIndexStatus(graph, "vsearch").status(SchemaStatus.ENABLED).timeout(60, ChronoUnit.SECONDS).call();
        org.janusgraph.graphdb.database.management.ManagementSystem
            .awaitGraphIndexStatus(graph, "esearch").status(SchemaStatus.ENABLED).timeout(60, ChronoUnit.SECONDS).call();

        idManager = ((StandardJanusGraph) graph).getIDManager();
        decoder = new DebeziumCassandraJsonDecoder((StandardJanusGraph) graph);
        applier = new MixedIndexUpdateApplier((StandardJanusGraph) graph, BACKING::equals);
        config = CdcWorkerConfiguration.builder()
            .bootstrapServers("dummy:9092").topics(Arrays.asList(TOPIC))
            .retryLimit(2).retryInitialWait(Duration.ofMillis(1)).retryMaxWait(Duration.ofMillis(2))
            .pollTimeout(Duration.ofMillis(10)).build();
    }

    @AfterEach
    public void tearDown() {
        if (graph != null && graph.isOpen()) graph.close();
    }

    // ---- driving the worker through a MockConsumer ----

    private void process(byte[]... eventValues) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        consumer.assign(Collections.singletonList(tp));
        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        long offset = 0;
        for (byte[] value : eventValues) {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, null, value));
        }
        new CdcIndexUpdateWorker(consumer, decoder, applier::apply, config).pollOnce();
    }

    private byte[] bytesOf(StaticBuffer b) {
        return b.as(StaticBuffer.ARRAY_FACTORY);
    }

    private byte[] partitionEvent(Object vertexId, String op) {
        return debeziumEvent(op, bytesOf(idManager.getKey(vertexId)), null, null);
    }

    private byte[] relationEvent(Object outVertexId, Entry entry, String op) {
        return debeziumEvent(op, bytesOf(idManager.getKey(outVertexId)), bytesOf(entry.getColumn()), bytesOf(entry.getValue()));
    }

    private byte[] debeziumEvent(String op, byte[] key, byte[] column1, byte[] value) {
        ObjectNode after = mapper.createObjectNode();
        after.set("key", cell(key));
        if (column1 != null) after.set("column1", cell(column1));
        if (value != null) after.set("value", cell(value));
        ObjectNode source = mapper.createObjectNode();
        source.put("keyspace", "janusgraph");
        source.put("table", "edgestore");
        ObjectNode payload = mapper.createObjectNode();
        payload.put("op", op);
        payload.set("source", source);
        payload.set("after", after);
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

    private ObjectNode cell(byte[] b) {
        ObjectNode c = mapper.createObjectNode();
        c.put("value", Base64.getEncoder().encodeToString(b));
        return c;
    }

    /** Serializes the first matching relation of a vertex into its edgestore Entry (column+value). */
    private Entry serializeRelation(Object vid, String edgeLabel) {
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        try {
            Vertex v = tx.getVertex((Long) vid);
            InternalRelation rel = edgeLabel != null
                ? (InternalRelation) v.edges(Direction.OUT, edgeLabel).next()
                : (InternalRelation) v.properties().next();
            return ((StandardJanusGraph) graph).getEdgeSerializer().writeRelation(rel, 0, tx);
        } finally {
            tx.rollback();
        }
    }

    private long vsearch(String name) {
        return graph.indexQuery("vsearch", "v.name:" + name).vertexStream().count();
    }

    private long esearch(String since) {
        return graph.indexQuery("esearch", "e.since:" + since).edgeStream().count();
    }

    // ---- scenarios ----

    @Test
    public void vertexAddConverges() {
        JanusGraphVertex v = graph.addVertex("name", "alice");
        graph.tx().commit();
        assertEquals(0L, vsearch("alice"), "cdc-only: not yet indexed");
        process(partitionEvent(v.id(), "i"));
        assertEquals(1L, vsearch("alice"));
    }

    @Test
    public void vertexUpdateConverges() {
        JanusGraphVertex v = graph.addVertex("name", "alice");
        graph.tx().commit();
        process(partitionEvent(v.id(), "i"));
        assertEquals(1L, vsearch("alice"));

        graph.traversal().V(v.id()).property("name", "alicia").iterate();
        graph.tx().commit();
        process(partitionEvent(v.id(), "u"));
        assertEquals(1L, vsearch("alicia"));
        assertEquals(0L, vsearch("alice"));
    }

    @Test
    public void vertexRemoveConverges() {
        JanusGraphVertex v = graph.addVertex("name", "alice");
        graph.tx().commit();
        process(partitionEvent(v.id(), "i"));
        assertEquals(1L, vsearch("alice"));

        graph.traversal().V(v.id()).drop().iterate();
        graph.tx().commit();
        process(partitionEvent(v.id(), "d"));
        assertEquals(0L, vsearch("alice"));
    }

    @Test
    public void staleEventStillConvergesToCurrentState() {
        JanusGraphVertex v = graph.addVertex("name", "v1");
        graph.tx().commit();
        // Build the event while the vertex is still "v1", then move it to "v2" before processing.
        byte[] staleEvent = partitionEvent(v.id(), "u");
        graph.traversal().V(v.id()).property("name", "v2").iterate();
        graph.tx().commit();

        process(staleEvent); // reindex reads CURRENT state -> v2, never the stale v1
        assertEquals(1L, vsearch("v2"));
        assertEquals(0L, vsearch("v1"));
    }

    @Test
    public void edgeAddConverges() {
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        a.addEdge("knows", b, "since", "2020");
        graph.tx().commit();
        assertEquals(0L, esearch("2020"));
        process(relationEvent(a.id(), serializeRelation(a.id(), "knows"), "i"));
        assertEquals(1L, esearch("2020"));
    }

    @Test
    public void edgeRemoveConverges() {
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        Edge e = a.addEdge("knows", b, "since", "2020");
        graph.tx().commit();
        process(relationEvent(a.id(), serializeRelation(a.id(), "knows"), "i"));
        assertEquals(1L, esearch("2020"));

        Entry capturedEdge = serializeRelation(a.id(), "knows"); // capture column+value before deletion
        graph.traversal().E(e.id()).drop().iterate();
        graph.tx().commit();
        process(relationEvent(a.id(), capturedEdge, "d"));
        assertEquals(0L, esearch("2020"));
    }
}
