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

import io.debezium.connector.cassandra.CassandraConnectorTaskTemplate;
import io.debezium.connector.cassandra.JanusGraphCdcConnectorStarter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.diskstorage.es.JanusGraphElasticsearchContainer;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.MixedIndexUpdateApplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The full real-container pipeline: JanusGraph (CQL with storage.cql.cdc=true) writes graph data to a real Cassandra
 * (CDC enabled); the real Debezium Cassandra connector (run embedded) captures edgestore changes into a real Kafka;
 * the real {@link CdcIndexUpdateWorker} consumes and reindexes into a real ElasticSearch. Asserts ES converges to the
 * graph for vertex add/update/remove. This is the end-to-end regression guard for the CDC feature (issue #4873).
 *
 * <p>Requires Java 17+ (Testcontainers 2.x and Debezium 3.x) and the cassandra-all module-open JVM flags (see the
 * surefire argLine in the module pom). The connector reads the Cassandra node's cdc_raw directory via a bind mount.</p>
 */
public class CdcCassandraDebeziumElasticsearchTest {

    private static final String KEYSPACE = "cdctest";
    private static final String BACKING = "search";
    private static final String TOPIC_PREFIX = "cdc";
    private static final String TOPIC = TOPIC_PREFIX + "." + KEYSPACE + ".edgestore";

    private static Path hostData;
    private static GenericContainer<?> cassandra;
    private static KafkaContainer kafka;
    private static JanusGraphElasticsearchContainer es;

    private static JanusGraph graph;
    private static CassandraConnectorTaskTemplate connector;
    private static CdcIndexUpdateWorker worker;

    @BeforeAll
    public static void startPipeline() throws Exception {
        // 1. Host dir shared with Cassandra so the embedded connector can read cdc_raw.
        hostData = Files.createTempDirectory("janusgraph-cass-cdc");
        for (String sub : new String[]{"data", "commitlog", "cdc_raw", "hints", "saved_caches"}) {
            Path p = Files.createDirectories(hostData.resolve(sub));
            p.toFile().setReadable(true, false);
            p.toFile().setWritable(true, false);
            p.toFile().setExecutable(true, false);
        }

        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));
        kafka.start();

        es = new JanusGraphElasticsearchContainer();
        es.start();

        cassandra = new GenericContainer<>(DockerImageName.parse("cassandra:4.1.7"))
            .withExposedPorts(9042)
            .withCopyFileToContainer(MountableFile.forClasspathResource("cassandra-cdc.yaml"), "/etc/cassandra/cassandra.yaml")
            .withFileSystemBind(hostData.toString(), "/var/lib/cassandra")
            .withEnv("MAX_HEAP_SIZE", "1G")
            .withEnv("HEAP_NEWSIZE", "512M")
            .waitingFor(Wait.forLogMessage(".*Startup complete.*", 1).withStartupTimeout(Duration.ofMinutes(5)));
        cassandra.start();

        // 2. Open JanusGraph on CQL (cdc=true on edgestore) + ElasticSearch (cdc-only).
        graph = openGraph();
        createSchema(graph);
        // Force creation of the edgestore table (with cdc=true) before the connector starts.
        graph.addVertex("name", "warmup");
        graph.tx().commit();
        cassandra.execInContainer("nodetool", "flush", KEYSPACE, "edgestore");

        // The embedded connector runs as the CI runner's uid but shares Cassandra's data tree via the bind mount, and
        // its cassandra-all reads the schema SSTables from it. Cassandra creates those directories as its own container
        // uid, so on Linux the connector's Keyspace.<clinit> -> DatabaseDescriptor.createAllDirectories intermittently
        // fails with "<dir>; unable to start server". Make the shared tree world-accessible (as root) before the
        // connector starts. (On macOS Docker remaps the uid, so this never reproduces locally.)
        cassandra.execInContainerWithUser("root", "chmod", "-R", "a+rwX", "/var/lib/cassandra");

        // 3. Start the embedded Debezium Cassandra connector (captures changes from now on).
        connector = startConnector();

        // 4. Start the CDC index-update worker.
        worker = startWorker();
    }

    @AfterAll
    public static void stopPipeline() throws Exception {
        if (worker != null) worker.close();
        if (connector != null) connector.stopAll();
        if (graph != null && graph.isOpen()) graph.close();
        if (cassandra != null) cassandra.stop();
        if (es != null) es.stop();
        if (kafka != null) kafka.stop();
    }

    private static JanusGraph openGraph() {
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
        cfg.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{cassandra.getHost()});
        cfg.set(GraphDatabaseConfiguration.STORAGE_PORT, cassandra.getMappedPort(9042));
        cfg.set(CQLConfigOptions.KEYSPACE, KEYSPACE);
        cfg.set(CQLConfigOptions.LOCAL_DATACENTER, "datacenter1");
        cfg.set(CQLConfigOptions.CDC, true);
        es.setConfiguration(cfg, BACKING);
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, BACKING);
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS, false, BACKING);
        return JanusGraphFactory.open(cfg.getConfiguration());
    }

    private static void createSchema(JanusGraph graph) throws InterruptedException {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey since = mgmt.makePropertyKey("since").dataType(String.class).make();
        mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex(BACKING);
        mgmt.buildIndex("esearch", org.apache.tinkerpop.gremlin.structure.Edge.class).addKey(since).buildMixedIndex(BACKING);
        mgmt.commit();
        org.janusgraph.graphdb.database.management.ManagementSystem
            .awaitGraphIndexStatus(graph, "vsearch").status(SchemaStatus.ENABLED).timeout(60, ChronoUnit.SECONDS).call();
        org.janusgraph.graphdb.database.management.ManagementSystem
            .awaitGraphIndexStatus(graph, "esearch").status(SchemaStatus.ENABLED).timeout(60, ChronoUnit.SECONDS).call();
    }

    private static CassandraConnectorTaskTemplate startConnector() throws Exception {
        // The connector parses cassandra.yaml in THIS JVM, so its directories must be the host bind-mount paths.
        String containerYaml = readResource("cassandra-cdc.yaml");
        String connectorYaml = containerYaml.replace("/var/lib/cassandra", hostData.toString());
        Path connectorYamlFile = Files.createTempFile("connector-cassandra", ".yaml");
        Files.write(connectorYamlFile, connectorYaml.getBytes(StandardCharsets.UTF_8));

        Path appConf = Files.createTempFile("application", ".conf");
        Files.write(appConf, ("datastax-java-driver {\n"
            + "  basic.contact-points = [ \"" + cassandra.getHost() + ":" + cassandra.getMappedPort(9042) + "\" ]\n"
            + "  basic.load-balancing-policy.local-datacenter = \"datacenter1\"\n"
            + "  basic.request.timeout = 20 seconds\n"
            + "}\n").getBytes(StandardCharsets.UTF_8));

        Path relocationDir = Files.createTempDirectory("dbz-relocation");
        Path offsetDir = Files.createTempDirectory("dbz-offsets");

        Properties props = new Properties();
        props.put("connector.name", "janusgraph-cdc-test");
        props.put("topic.prefix", TOPIC_PREFIX);
        props.put("cassandra.config", connectorYamlFile.toString());
        props.put("cassandra.driver.config.file", appConf.toString());
        props.put("commit.log.relocation.dir", relocationDir.toString());
        props.put("offset.backing.store.dir", offsetDir.toString());
        props.put("commit.log.real.time.processing.enabled", "true");
        props.put("commit.log.marked.complete.poll.interval.ms", "1000");
        props.put("cdc.dir.poll.interval.ms", "1000");
        props.put("snapshot.mode", "NEVER");
        props.put("snapshot.consistency", "ONE");
        props.put("http.port", String.valueOf(freePort()));
        props.put("num.of.change.event.queues", "1");
        props.put("kafka.producer.bootstrap.servers", kafka.getBootstrapServers());
        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("key.converter.schemas.enable", "false");
        props.put("value.converter.schemas.enable", "false");

        return JanusGraphCdcConnectorStarter.startEmbedded(io.debezium.config.Configuration.from(props));
    }

    private static CdcIndexUpdateWorker startWorker() {
        StandardJanusGraph sjg = (StandardJanusGraph) graph;
        CdcWorkerConfiguration config = CdcWorkerConfiguration.builder()
            .bootstrapServers(kafka.getBootstrapServers())
            .topics(Arrays.asList(TOPIC))
            .groupId("cdc-full-e2e-" + System.nanoTime())
            .pollTimeout(Duration.ofMillis(250))
            .retryInitialWait(Duration.ofMillis(100))
            .retryMaxWait(Duration.ofSeconds(2))
            .build();
        CdcIndexUpdateWorker w = new CdcIndexUpdateWorker(
            new KafkaConsumer<>(config.toConsumerProperties()),
            new DebeziumCassandraJsonDecoder(sjg),
            new MixedIndexUpdateApplier(sjg, Collections.singleton(BACKING)::contains)::apply,
            config);
        w.start();
        return w;
    }

    @Test
    public void vertexLifecycleConvergesThroughCassandraCdcDebeziumKafkaElasticsearch() throws Exception {
        JanusGraphVertex v = graph.addVertex("name", "alice");
        graph.tx().commit();
        awaitConverged(() -> vsearch("alice"), 1);

        graph.traversal().V(v.id()).property("name", "alicia").iterate();
        graph.tx().commit();
        awaitConverged(() -> vsearch("alicia"), 1);
        awaitConverged(() -> vsearch("alice"), 0);

        // Remove the indexed property but keep the vertex: the real Debezium delete envelope carries no value bytes,
        // and the vertex must be reindexed to a state without the property (stale document removed).
        graph.traversal().V(v.id()).properties("name").drop().iterate();
        graph.tx().commit();
        awaitConverged(() -> vsearch("alicia"), 0);

        // Re-add an indexed value so the vertex has a live document again: the delete assertion below is then
        // load-bearing (without this, vsearch is already 0 after the property drop and the await cannot fail).
        graph.traversal().V(v.id()).property("name", "alison").iterate();
        graph.tx().commit();
        awaitConverged(() -> vsearch("alison"), 1);

        graph.traversal().V(v.id()).drop().iterate();
        graph.tx().commit();
        awaitConverged(() -> vsearch("alison"), 0);
    }

    @Test
    public void edgeLifecycleConvergesThroughCassandraCdcDebeziumKafkaElasticsearch() throws Exception {
        // Edge add + remove against the REAL pipeline: the remove is the critical case, because a real Cassandra
        // delete tombstone carries no value bytes and the edge identity must be recovered from the column alone.
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        org.apache.tinkerpop.gremlin.structure.Edge e = a.addEdge("knows", b, "since", "e2e2020");
        graph.tx().commit();
        awaitConverged(() -> esearch("e2e2020"), 1);

        graph.traversal().E(e.id()).drop().iterate();
        graph.tx().commit();
        awaitConverged(() -> esearch("e2e2020"), 0);
    }

    private long vsearch(String name) {
        return graph.indexQuery("vsearch", "v.name:" + name).vertexTotals();
    }

    private long esearch(String since) {
        return graph.indexQuery("esearch", "e.since:" + since).edgeTotals();
    }

    private void awaitConverged(LongSupplier actual, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000;
        while (System.currentTimeMillis() < deadline) {
            if (actual.getAsLong() == expected) {
                return;
            }
            // Force the CDC commitlog segment to surface into cdc_raw, then give the pipeline time.
            cassandra.execInContainer("nodetool", "flush", KEYSPACE, "edgestore");
            Thread.sleep(2000);
        }
        assertEquals(expected, actual.getAsLong());
    }

    private static String readResource(String name) throws Exception {
        try (InputStream in = CdcCassandraDebeziumElasticsearchTest.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("Resource not found: " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
