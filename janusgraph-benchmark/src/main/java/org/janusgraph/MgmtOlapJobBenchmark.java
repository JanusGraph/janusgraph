// Copyright 2021 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.es.ElasticMajorVersion;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.es.ElasticSearchSetup;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * This benchmark evaluates performance of OLAP jobs that
 * can be run via the ManagementSystem interface, including:
 * 1) REINDEX
 * 2) DISCARD_INDEX
 *
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MgmtOlapJobBenchmark {
    @Param("10000")
    int size;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        return config.getConfiguration();
    }

    @Setup(Level.Iteration)
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());

        ((StandardJanusGraph) graph).getOpenTransactions().forEach(JanusGraphTransaction::rollback);

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameIndex").status(SchemaStatus.ENABLED).call();

        for (int j = 0; j < size; j++) {
            graph.addVertex("name", "value" + j, "alias", "value" + j);
        }
        graph.tx().commit();

        mgmt = graph.openManagement();
        mgmt.buildIndex("aliasIndex", Vertex.class).addKey(mgmt.getPropertyKey("alias")).buildCompositeIndex();
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "aliasIndex").status(SchemaStatus.ENABLED).call();

        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("nameIndex"), SchemaAction.DISABLE_INDEX).get();
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameIndex").status(SchemaStatus.DISABLED).call();
    }

    @Benchmark
    public void runReindex(Blackhole blackhole) throws ExecutionException, InterruptedException {
        JanusGraphManagement mgmt = graph.openManagement();
        blackhole.consume(mgmt.updateIndex(mgmt.getGraphIndex("aliasIndex"), SchemaAction.REINDEX).get());
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "aliasIndex").status(SchemaStatus.ENABLED).call();
    }

    @Benchmark
    public void runClearIndex(Blackhole blackhole) throws ExecutionException, InterruptedException {
        JanusGraphManagement mgmt = graph.openManagement();
        blackhole.consume(mgmt.updateIndex(mgmt.getGraphIndex("nameIndex"), SchemaAction.DISCARD_INDEX).get());
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameIndex").status(SchemaStatus.DISCARDED).call();
    }


    @TearDown(Level.Iteration)
    public void tearDown() {
        graph.close();
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(MgmtOlapJobBenchmark.class.getSimpleName())
            .warmupIterations(1)
            .measurementIterations(3)
            .build();
        new Runner(options).run();
    }

    @BenchmarkMode(Mode.AverageTime)
    @Fork(1)
    @State(Scope.Benchmark)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class ElasticSearchMixedIndexReindexBenchmark {

        private static final int ELASTIC_PORT = 9200;
        private static final String DEFAULT_ELASTICSEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";
        private static final String DEFAULT_ELASTICSEARCH_VERSION = "9.0.3";
        private static final String INDEX_BACKEND_NAME = "search";
        private static final String INDEX_NAME = "nameMixed";

        private static final ElasticsearchContainer ELASTICSEARCH = createElasticsearchContainer();
        private static boolean shutdownHookRegistered;

        @Param("10000")
        int size;

        @Param({"false", "true"})
        boolean mixedIndexReindexBatchEnabled;

        @Param("1000")
        int mixedIndexReindexBatchSize;

        @Param("1")
        int reindexThreads;

        @Param("wait_for")
        String bulkRefresh;

        @Param("1")
        int numberOfShards;

        // Storage backend: "inmemory" (default) measures the index-write path in isolation; "cql" measures
        // a realistic reindex against Cassandra (real storage scan + entry deserialization).
        @Param("inmemory")
        String storageBackend;

        // Storage scan page size (rows fetched per backend round trip during the reindex scan).
        @Param("100")
        int storagePageSize;

        // Number of token ranges scanned concurrently on CQL (1 = single sequential coordinator scan).
        @Param("1")
        int cqlParallelScanRanges;

        JanusGraph graph;
        String indexPrefix;
        String keyspace;

        public WriteConfiguration getConfiguration() {
            ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
            configureStorageBackend(config);
            config.set(GraphDatabaseConfiguration.INDEX_BACKEND, "elasticsearch", INDEX_BACKEND_NAME);
            config.set(GraphDatabaseConfiguration.INDEX_NAME, indexPrefix, INDEX_BACKEND_NAME);
            config.set(GraphDatabaseConfiguration.INDEX_HOSTS, new String[]{getEsHost()}, INDEX_BACKEND_NAME);
            config.set(GraphDatabaseConfiguration.INDEX_PORT, getEsPort(), INDEX_BACKEND_NAME);
            config.set(ElasticSearchIndex.INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), INDEX_BACKEND_NAME);
            config.set(ElasticSearchIndex.BULK_REFRESH, bulkRefresh, INDEX_BACKEND_NAME);
            config.set(ElasticSearchIndex.NUMBER_OF_SHARDS, numberOfShards, INDEX_BACKEND_NAME);
            config.set(ElasticSearchIndex.NUMBER_OF_REPLICAS, 0, INDEX_BACKEND_NAME);
            config.set(GraphDatabaseConfiguration.MIXED_INDEX_REINDEX_BATCH_ENABLED, mixedIndexReindexBatchEnabled);
            config.set(GraphDatabaseConfiguration.MIXED_INDEX_REINDEX_BATCH_SIZE, mixedIndexReindexBatchSize);
            config.set(GraphDatabaseConfiguration.PAGE_SIZE, storagePageSize);
            return config.getConfiguration();
        }

        private void configureStorageBackend(ModifiableConfiguration config) {
            if ("cql".equals(storageBackend)) {
                config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
                config.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{getCqlHost()});
                config.set(GraphDatabaseConfiguration.STORAGE_PORT, getCqlPort());
                config.set(CQLConfigOptions.KEYSPACE, keyspace);
                config.set(CQLConfigOptions.LOCAL_DATACENTER, System.getProperty("bench.cql.dc", "datacenter1"));
                config.set(CQLConfigOptions.PARALLEL_SCAN_TOKEN_RANGES, cqlParallelScanRanges);
            } else {
                config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
            }
        }

        private static String getCqlHost() {
            return System.getProperty("bench.cql.host", "127.0.0.1");
        }

        private static int getCqlPort() {
            return Integer.parseInt(System.getProperty("bench.cql.port", "9042"));
        }

        @Setup(Level.Iteration)
        public void setUp() throws Exception {
            startElasticsearch();
            indexPrefix = "janusgraphbenchmark" + Long.toString(System.nanoTime(), 36);
            keyspace = "jgbench" + Long.toString(System.nanoTime(), 36);
            System.err.println("BENCH_KEYSPACE=" + keyspace);
            graph = JanusGraphFactory.open(getConfiguration());

            JanusGraphManagement management = graph.openManagement();
            management.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            management.commit();

            IntStream.range(0, size).forEach(vertexNumber -> graph.addVertex("name", "value" + vertexNumber));
            graph.tx().commit();

            management = graph.openManagement();
            PropertyKey name = management.getPropertyKey("name");
            management.buildIndex(INDEX_NAME, Vertex.class).addKey(name).buildMixedIndex(INDEX_BACKEND_NAME);
            management.commit();

            management = graph.openManagement();
            management.updateIndex(management.getGraphIndex(INDEX_NAME), SchemaAction.REGISTER_INDEX).get();
            management.commit();
            ManagementSystem.awaitGraphIndexStatus(graph, INDEX_NAME).status(SchemaStatus.REGISTERED).call();
        }

        @Benchmark
        public void runMixedIndexReindex(Blackhole blackhole) throws ExecutionException, InterruptedException {
            final long t0 = System.nanoTime();
            JanusGraphManagement management = graph.openManagement();
            ScanMetrics metrics = management.updateIndex(management.getGraphIndex(INDEX_NAME), SchemaAction.REINDEX, reindexThreads).get();
            blackhole.consume(metrics);
            management.commit();
            final long t1 = System.nanoTime();
            ManagementSystem.awaitGraphIndexStatus(graph, INDEX_NAME).status(SchemaStatus.ENABLED).call();
            final long t2 = System.nanoTime();
            // docUpdates = number of documents sent to the index = number of vertices indexed; it must equal
            // `size` for a correct scan (a token-range tiling gap would lose vertices, an overlap would duplicate).
            System.err.println(String.format("REINDEX_TIMING size=%d batch=%b/%d threads=%d page=%d cqlRanges=%d scanWrite=%.0fms await=%.0fms docUpdates=%d",
                size, mixedIndexReindexBatchEnabled, mixedIndexReindexBatchSize, reindexThreads, storagePageSize, cqlParallelScanRanges,
                (t1 - t0) / 1e6, (t2 - t1) / 1e6, metrics.getCustom("doc-updates")));
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws BackendException {
            if (graph != null && graph.isOpen()) {
                if ("cql".equals(storageBackend) && !Boolean.getBoolean("bench.cql.keepKeyspace")) {
                    // Drop the per-iteration keyspace so repeated runs do not accumulate Cassandra state.
                    JanusGraphFactory.drop(graph);
                } else {
                    graph.close();
                }
            }
        }

        public static void main(String[] args) throws RunnerException {
            Options options = new OptionsBuilder()
                .include(ElasticSearchMixedIndexReindexBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .build();
            new Runner(options).run();
        }

        private static String externalEsHost() {
            return System.getProperty("bench.es.host");
        }

        private static String getEsHost() {
            final String external = externalEsHost();
            return external != null ? external : ELASTICSEARCH.getHost();
        }

        private static int getEsPort() {
            final String external = externalEsHost();
            return external != null ? Integer.parseInt(System.getProperty("bench.es.port", "9200"))
                                    : ELASTICSEARCH.getMappedPort(ELASTIC_PORT);
        }

        private static synchronized void startElasticsearch() {
            if (externalEsHost() != null) {
                return;
            }
            if (!ELASTICSEARCH.isRunning()) {
                ELASTICSEARCH.start();
                if (!shutdownHookRegistered) {
                    Runtime.getRuntime().addShutdownHook(new Thread(ELASTICSEARCH::stop));
                    shutdownHookRegistered = true;
                }
            }
        }

        private static ElasticsearchContainer createElasticsearchContainer() {
            ElasticsearchContainer container = new ElasticsearchContainer(getElasticsearchImage() + ":" + getElasticsearchVersion());
            container.withEnv("transport.host", "0.0.0.0");
            container.withEnv("xpack.security.enabled", "false");
            container.withEnv("action.destructive_requires_name", "false");
            if (ElasticMajorVersion.parse(getElasticsearchVersion()).getValue() > 6) {
                container.withEnv("ingest.geoip.downloader.enabled", "false");
            }
            container.withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
            return container;
        }

        private static String getElasticsearchImage() {
            return System.getProperty("elasticsearch.docker.image", DEFAULT_ELASTICSEARCH_IMAGE);
        }

        private static String getElasticsearchVersion() {
            return System.getProperty("elasticsearch.docker.version", DEFAULT_ELASTICSEARCH_VERSION);
        }
    }

}
