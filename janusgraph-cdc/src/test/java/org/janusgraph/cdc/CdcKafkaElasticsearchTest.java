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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import org.janusgraph.diskstorage.es.JanusGraphElasticsearchContainer;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.MixedIndexUpdateApplier;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Real-infrastructure component E2E: a real Kafka and a real ElasticSearch (Testcontainers), the real worker and
 * decoder, and a cdc-only graph whose mixed index lives in ElasticSearch. Debezium-format records (built from real
 * JanusGraph edgestore bytes) are published to a real Kafka topic; the worker consumes them and reindexes into
 * ElasticSearch. Asserts ES converges to the graph for vertex and edge add/update/remove. This exercises the entire
 * JanusGraph-owned chain on real infrastructure (the only piece not covered here vs. the full pipeline is the
 * Cassandra-CDC-&gt;Debezium hop, which {@code CdcCassandraDebeziumElasticsearchTest} covers).
 */
@Testcontainers
public class CdcKafkaElasticsearchTest {

    private static final String TOPIC = "cassandra.janusgraph.edgestore";

    @Container
    private static final JanusGraphElasticsearchContainer ES = new JanusGraphElasticsearchContainer();

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    private final ObjectMapper mapper = new ObjectMapper();
    private JanusGraph graph;
    private IDManager idManager;
    private DebeziumCassandraJsonDecoder decoder;
    private MixedIndexUpdateApplier applier;
    private CdcIndexUpdateWorker worker;
    private KafkaProducer<byte[], byte[]> producer;

    @BeforeEach
    public void setUp() throws Exception {
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        ES.setConfiguration(cfg, "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS, false, "search"); // cdc-only
        graph = JanusGraphFactory.open(cfg.getConfiguration());

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey since = mgmt.makePropertyKey("since").dataType(String.class).make();
        mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex("search");
        mgmt.buildIndex("esearch", Edge.class).addKey(since).buildMixedIndex("search");
        mgmt.commit();
        org.janusgraph.graphdb.database.management.ManagementSystem
            .awaitGraphIndexStatus(graph, "vsearch").status(SchemaStatus.ENABLED).timeout(30, ChronoUnit.SECONDS).call();
        org.janusgraph.graphdb.database.management.ManagementSystem
            .awaitGraphIndexStatus(graph, "esearch").status(SchemaStatus.ENABLED).timeout(30, ChronoUnit.SECONDS).call();

        idManager = ((StandardJanusGraph) graph).getIDManager();
        decoder = new DebeziumCassandraJsonDecoder((StandardJanusGraph) graph);
        applier = new MixedIndexUpdateApplier((StandardJanusGraph) graph, "search"::equals);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        CdcWorkerConfiguration workerConfig = CdcWorkerConfiguration.builder()
            .bootstrapServers(KAFKA.getBootstrapServers())
            .topics(Arrays.asList(TOPIC))
            .groupId("cdc-it-" + System.nanoTime())
            .pollTimeout(Duration.ofMillis(200))
            .retryInitialWait(Duration.ofMillis(50))
            .retryMaxWait(Duration.ofMillis(500))
            .build();
        worker = new CdcIndexUpdateWorker(new KafkaConsumer<>(workerConfig.toConsumerProperties()), decoder,
            applier::apply, workerConfig);
        worker.start();
    }

    @AfterEach
    public void tearDown() {
        if (worker != null) worker.close();
        if (producer != null) producer.close();
        if (graph != null && graph.isOpen()) graph.close();
    }

    @Test
    public void vertexLifecycleConvergesOnRealKafkaAndElasticsearch() throws Exception {
        JanusGraphVertex v = graph.addVertex("name", "alice");
        graph.tx().commit();
        publish(partitionEvent(v.id(), "i"));
        await(() -> vsearch("alice"), 1);

        graph.traversal().V(v.id()).property("name", "alicia").iterate();
        graph.tx().commit();
        publish(partitionEvent(v.id(), "u"));
        await(() -> vsearch("alicia"), 1);
        await(() -> vsearch("alice"), 0);

        graph.traversal().V(v.id()).drop().iterate();
        graph.tx().commit();
        publish(partitionEvent(v.id(), "d"));
        await(() -> vsearch("alicia"), 0);
    }

    @Test
    public void edgeLifecycleConvergesOnRealKafkaAndElasticsearch() throws Exception {
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        Edge e = a.addEdge("knows", b, "since", "2020");
        graph.tx().commit();
        publish(relationEvent(a.id(), serializeRelation(a.id(), "knows"), "i"));
        await(() -> esearch("2020"), 1);

        Entry captured = serializeRelation(a.id(), "knows");
        graph.traversal().E(e.id()).drop().iterate();
        graph.tx().commit();
        publish(relationEvent(a.id(), captured, "d"));
        await(() -> esearch("2020"), 0);
    }

    // ---- helpers ----

    // Use *Totals() (index hit counts) rather than *Stream() so a transiently-stale index document for an
    // already-deleted element does not NPE while resolving graph elements during convergence polling.
    private long vsearch(String name) {
        return graph.indexQuery("vsearch", "v.name:" + name).vertexTotals();
    }

    private long esearch(String since) {
        return graph.indexQuery("esearch", "e.since:" + since).edgeTotals();
    }

    private void await(LongSupplier actual, long expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (actual.getAsLong() == expected) return;
            Thread.sleep(250);
        }
        assertEquals(expected, actual.getAsLong());
    }

    private void publish(byte[] value) throws Exception {
        producer.send(new ProducerRecord<>(TOPIC, null, value)).get();
        producer.flush();
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
}
