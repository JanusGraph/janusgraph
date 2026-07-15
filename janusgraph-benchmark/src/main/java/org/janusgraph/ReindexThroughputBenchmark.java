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

package org.janusgraph;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Standalone (non-JMH) throughput benchmark for {@code SchemaAction.REINDEX} of an Elasticsearch
 * mixed index over an existing CQL-backed graph. Mirrors the production db-migration scenario:
 * a keyspace already holding N vertices gets a NEW mixed index which is then REGISTERED and
 * REINDEXed, and we measure wall-clock time and records/min of the reindex scan.
 *
 * Requires a reachable Cassandra and Elasticsearch (no testcontainers management):
 *   -Dbench.cql.host=127.0.0.1 -Dbench.cql.port=9042 -Dbench.cql.dc=datacenter1
 *   -Dbench.es.host=127.0.0.1 -Dbench.es.port=9200
 *
 * Data shape (deterministic, reusable across processes):
 *   name=value&lt;i&gt; plus &lt;extraProps&gt; extra properties (mix of short strings, long text-ish
 *   strings and longs) and &lt;edgesPerVertex&gt; out-edges per vertex to fatten the adjacency rows
 *   the way real graphs do (edges are NOT indexed but they are stored in the same table the
 *   reindex scan reads).
 *
 * Main knobs (system properties, all optional):
 *   bench.size=100000            number of vertices
 *   bench.extraProps=10          extra properties per vertex
 *   bench.indexedProps=4         how many of them the new mixed index covers (plus "name")
 *   bench.edgesPerVertex=3       out-edges per vertex (fixed-degree mode)
 *   bench.edges.skewed=false     skewed out-degree distribution instead of a fixed degree:
 *                                most vertices get 0-3 edges, a long tail gets up to
 *                                bench.edges.max (supernodes), average ~4 - the shape wide
 *                                real-world graphs have
 *   bench.edges.max=5000         maximum out-degree in skewed mode
 *   bench.threads=100            numOfThreads passed to updateIndex(...)
 *   bench.pageSize=200           storage.page-size
 *   bench.scanPageSize=0         storage.cql.scan-page-size (0 = keep storage.page-size)
 *   bench.ranges=1               storage.cql.parallel-scan-token-ranges
 *   bench.batchSize=1000         schema.reindex.mixed-index-batch-size
 *   bench.keyspace=reindexbench  Cassandra keyspace (also used as ES index prefix)
 *   bench.seedOnly=false         seed the data and exit
 *   bench.reuse=true             reuse existing keyspace data when the vertex count matches
 *   bench.drop=false             drop the keyspace before doing anything else
 *   bench.runs=1                 how many reindex runs (each creates a fresh index)
 */
public class ReindexThroughputBenchmark {

    private static final String INDEX_BACKEND_NAME = "search";
    private static final String EDGE_LABEL = "rel";
    private static final String VERTEX_LABEL = "entity";
    private static final int SEED_TX_SIZE = 2000;
    // Shared by the config-building and the RESULT-echo sites so the reported knobs are the used ones.
    private static final int DEFAULT_PAGE_SIZE = 200;
    private static final int DEFAULT_SCAN_PAGE_SIZE = 0;
    private static final int DEFAULT_RANGES = 1;
    private static final int DEFAULT_BATCH_SIZE = 1000;

    /** Outcome of one timed REINDEX run, for callers (tests) that assert on it. */
    public static final class Result {
        public final String indexName;
        public final double seconds;
        public final long success;
        public final long failure;
        public final long docUpdates;
        public final int size;

        Result(String indexName, double seconds, long success, long failure, long docUpdates, int size) {
            this.indexName = indexName;
            this.seconds = seconds;
            this.success = success;
            this.failure = failure;
            this.docUpdates = docUpdates;
            this.size = size;
        }
    }

    public static void main(String[] args) throws Exception {
        runFromSystemProperties();
    }

    /**
     * Runs the benchmark as configured by the {@code bench.*} system properties and returns one
     * {@link Result} per reindex run (empty in seed-only mode). Reusable from manually-enabled tests.
     */
    public static List<Result> runFromSystemProperties() throws Exception {
        final int size = Integer.getInteger("bench.size", 100_000);
        final int extraProps = Integer.getInteger("bench.extraProps", 10);
        final int indexedProps = Integer.getInteger("bench.indexedProps", 4);
        final int edgesPerVertex = Integer.getInteger("bench.edgesPerVertex", 3);
        final boolean skewedEdges = Boolean.getBoolean("bench.edges.skewed");
        final int maxEdges = Integer.getInteger("bench.edges.max", 5000);
        final int threads = Integer.getInteger("bench.threads", 100);
        final int runs = Integer.getInteger("bench.runs", 1);
        final boolean seedOnly = Boolean.getBoolean("bench.seedOnly");
        final boolean drop = Boolean.getBoolean("bench.drop");
        final boolean reuse = !"false".equalsIgnoreCase(System.getProperty("bench.reuse", "true"));
        final String keyspace = System.getProperty("bench.keyspace", "reindexbench");
        // The reusability marker must describe the data SHAPE, not just the count: reusing a keyspace
        // seeded with a different shape would make measurements incomparable.
        final String shape = "v2:size=" + size + ",extraProps=" + extraProps
            + (skewedEdges ? ",skewedEdges(max=" + maxEdges + ")" : ",edgesPerVertex=" + edgesPerVertex);

        if (drop) {
            JanusGraph toDrop = JanusGraphFactory.open(buildConfiguration(keyspace));
            JanusGraphFactory.drop(toDrop);
            System.out.println("Dropped keyspace " + keyspace);
            // Drop-only mode (-Dbench.drop=true -Dbench.seedOnly=true -Dbench.size=0): stop after the drop.
            if (seedOnly && size == 0) return new ArrayList<>();
        }

        final List<Result> results = new ArrayList<>();
        JanusGraph graph = JanusGraphFactory.open(buildConfiguration(keyspace));
        try {
            ensureSchema(graph, extraProps);
            String existing = readShapeMarker(graph);
            if (!reuse || !shape.equals(existing)) {
                // Drop unconditionally on mismatch: an absent marker does NOT mean an empty keyspace
                // (it may hold data seeded by another driver version), and seeding on top of existing
                // data silently inflates every subsequent measurement.
                System.out.printf("Seed marker [%s] does not match requested shape [%s]; dropping keyspace %s and reseeding.%n",
                    existing, shape, keyspace);
                JanusGraphFactory.drop(graph);
                graph = JanusGraphFactory.open(buildConfiguration(keyspace));
                ensureSchema(graph, extraProps);
                seed(graph, size, extraProps, edgesPerVertex, skewedEdges, maxEdges);
                graph.variables().set("reindexBenchShape", shape);
            } else {
                System.out.printf("Reusing existing keyspace %s (%s).%n", keyspace, shape);
            }

            if (seedOnly) {
                System.out.println("Seed-only mode, exiting.");
                return results;
            }

            for (int run = 0; run < runs; run++) {
                results.add(runReindex(graph, size, extraProps, indexedProps, threads));
            }
        } finally {
            graph.close();
        }
        return results;
    }

    private static WriteConfiguration buildConfiguration(String keyspace) {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{System.getProperty("bench.cql.host", "127.0.0.1")});
        config.set(GraphDatabaseConfiguration.STORAGE_PORT, Integer.getInteger("bench.cql.port", 9042));
        config.set(CQLConfigOptions.KEYSPACE, keyspace);
        config.set(CQLConfigOptions.LOCAL_DATACENTER, System.getProperty("bench.cql.dc", "datacenter1"));
        config.set(CQLConfigOptions.READ_CONSISTENCY, System.getProperty("bench.cql.readCL", "ONE"));
        config.set(CQLConfigOptions.WRITE_CONSISTENCY, System.getProperty("bench.cql.writeCL", "ONE"));
        config.set(CQLConfigOptions.LOCAL_MAX_CONNECTIONS_PER_HOST, 2);
        config.set(CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION, 1024);

        // Reindex / scan knobs under test.
        config.set(GraphDatabaseConfiguration.PAGE_SIZE, Integer.getInteger("bench.pageSize", DEFAULT_PAGE_SIZE));
        config.set(CQLConfigOptions.PARALLEL_SCAN_TOKEN_RANGES, Integer.getInteger("bench.ranges", DEFAULT_RANGES));
        config.set(GraphDatabaseConfiguration.MIXED_INDEX_REINDEX_BATCH_ENABLED, true);
        config.set(GraphDatabaseConfiguration.MIXED_INDEX_REINDEX_BATCH_SIZE, Integer.getInteger("bench.batchSize", DEFAULT_BATCH_SIZE));
        final int scanPageSize = Integer.getInteger("bench.scanPageSize", DEFAULT_SCAN_PAGE_SIZE);
        if (scanPageSize > 0) {
            config.set(CQLConfigOptions.SCAN_PAGE_SIZE, scanPageSize);
        }
        final String perPartitionLimit = System.getProperty("bench.perPartitionLimit");
        if (perPartitionLimit != null) {
            config.set(CQLConfigOptions.SCAN_PER_PARTITION_LIMIT_ENABLED, Boolean.parseBoolean(perPartitionLimit));
        }

        // Elasticsearch index backend, mirroring prod (1 shard; replicas 0 because single node).
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND, "elasticsearch", INDEX_BACKEND_NAME);
        config.set(GraphDatabaseConfiguration.INDEX_NAME, keyspace, INDEX_BACKEND_NAME);
        config.set(GraphDatabaseConfiguration.INDEX_HOSTS, new String[]{System.getProperty("bench.es.host", "127.0.0.1")}, INDEX_BACKEND_NAME);
        config.set(GraphDatabaseConfiguration.INDEX_PORT, Integer.getInteger("bench.es.port", 9200), INDEX_BACKEND_NAME);
        config.set(ElasticSearchIndex.NUMBER_OF_SHARDS, Integer.getInteger("bench.esShards", 1), INDEX_BACKEND_NAME);
        config.set(ElasticSearchIndex.NUMBER_OF_REPLICAS, 0, INDEX_BACKEND_NAME);

        // Bulk-load friendly settings comparable to the prod migration job.
        config.set(GraphDatabaseConfiguration.STORAGE_BATCH, true);
        config.set(GraphDatabaseConfiguration.AUTO_TYPE, "none");
        config.set(GraphDatabaseConfiguration.IDS_BLOCK_SIZE, 1_000_000);
        config.set(GraphDatabaseConfiguration.BUFFER_SIZE, 2000);
        return config.getConfiguration();
    }

    private static void ensureSchema(JanusGraph graph, int extraProps) {
        JanusGraphManagement mgmt = graph.openManagement();
        try {
            boolean changed = false;
            if (mgmt.getPropertyKey("name") == null) {
                mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
                changed = true;
            }
            for (int p = 0; p < extraProps; p++) {
                if (mgmt.getPropertyKey(propName(p)) == null) {
                    mgmt.makePropertyKey(propName(p)).dataType(p % 3 == 2 ? Long.class : String.class)
                        .cardinality(Cardinality.SINGLE).make();
                    changed = true;
                }
            }
            if (mgmt.getEdgeLabel(EDGE_LABEL) == null) {
                mgmt.makeEdgeLabel(EDGE_LABEL).make();
                changed = true;
            }
            // A real (non-default) vertex label makes the reindex scan's vertex-label slice query
            // return one row per vertex, as in production graphs.
            if (mgmt.getVertexLabel(VERTEX_LABEL) == null) {
                mgmt.makeVertexLabel(VERTEX_LABEL).make();
                changed = true;
            }
            if (changed) {
                mgmt.commit();
            } else {
                mgmt.rollback();
            }
        } catch (RuntimeException e) {
            mgmt.rollback();
            throw e;
        }
    }

    private static String propName(int p) {
        return "prop" + p;
    }

    /** A graph variable records the seeded data shape so reruns can safely reuse data (O(1) read). */
    private static String readShapeMarker(JanusGraph graph) {
        try {
            final Object shape = graph.variables().get("reindexBenchShape").orElse("");
            graph.tx().rollback();
            return String.valueOf(shape);
        } catch (RuntimeException e) {
            graph.tx().rollback();
            return "";
        }
    }

    private static void seed(JanusGraph graph, int size, int extraProps, int edgesPerVertex,
                             boolean skewedEdges, int maxEdges) {
        System.out.printf("Seeding %d vertices (extraProps=%d, edges=%s)...%n", size, extraProps,
            skewedEdges ? "skewed(max=" + maxEdges + ")" : "fixed(" + edgesPerVertex + ")");
        final long start = System.nanoTime();
        // Keep a sliding window of recent vertices to attach edges to without extra lookups.
        final List<JanusGraphVertex> window = new ArrayList<>(SEED_TX_SIZE + 64);
        long edges = 0;
        long lastLog = System.nanoTime();
        for (int base = 0; base < size; base += SEED_TX_SIZE) {
            final int end = Math.min(base + SEED_TX_SIZE, size);
            window.clear();
            for (int i = base; i < end; i++) {
                final Random rnd = new Random(i); // deterministic per vertex
                final Object[] props = new Object[4 + extraProps * 2];
                int pos = 0;
                props[pos++] = T.label;
                props[pos++] = VERTEX_LABEL;
                props[pos++] = "name";
                props[pos++] = "value" + i;
                for (int p = 0; p < extraProps; p++) {
                    props[pos++] = propName(p);
                    switch (p % 3) {
                        case 0: props[pos++] = "s" + p + "-" + i + "-" + Long.toString(rnd.nextLong(), 36); break;
                        case 1: props[pos++] = longishText(rnd, i, p); break;
                        default: props[pos++] = rnd.nextLong(); break;
                    }
                }
                final JanusGraphVertex v = graph.addVertex(props);
                final int outDegree = skewedEdges ? skewedOutDegree(rnd, maxEdges) : edgesPerVertex;
                for (int e = 0; e < outDegree && !window.isEmpty(); e++) {
                    v.addEdge(EDGE_LABEL, window.get(rnd.nextInt(window.size())));
                    edges++;
                }
                window.add(v);
            }
            graph.tx().commit();
            if (System.nanoTime() - lastLog > 5_000_000_000L) {
                lastLog = System.nanoTime();
                System.out.printf("  seeded %d/%d vertices, %d edges (%.0f v/s)%n",
                    end, size, edges, end / ((System.nanoTime() - start) / 1e9));
            }
        }
        graph.tx().commit();
        System.out.printf("Seeding done in %.1fs: %d vertices, %d edges%n",
            (System.nanoTime() - start) / 1e9, size, edges);
    }

    /**
     * Long-tailed out-degree: most vertices get 0-3 edges, ~14%% get 4-19, ~0.9%% get 20-99,
     * ~0.09%% get 100-999 and ~0.01%% become supernodes with 1000..maxEdges edges (average ~4.2 at
     * maxEdges=5000). Wide adjacency rows are exactly what the scan-side per-partition-limit
     * pushdown is about, so the tail matters more than the average.
     */
    private static int skewedOutDegree(Random rnd, int maxEdges) {
        final double r = rnd.nextDouble();
        if (r < 0.85) return rnd.nextInt(4);
        if (r < 0.99) return 4 + rnd.nextInt(16);
        if (r < 0.999) return 20 + rnd.nextInt(80);
        if (r < 0.9999) return 100 + rnd.nextInt(900);
        return 1000 + rnd.nextInt(Math.max(1, maxEdges - 999));
    }

    private static String longishText(Random rnd, int i, int p) {
        final StringBuilder sb = new StringBuilder(120);
        sb.append("Vertex ").append(i).append(" property ").append(p).append(" lorem ");
        for (int w = 0; w < 12; w++) {
            sb.append(Long.toString(Math.abs(rnd.nextLong()), 36)).append(' ');
        }
        return sb.toString();
    }

    private static Result runReindex(JanusGraph graph, int size, int extraProps, int indexedProps, int threads) throws Exception {
        final String indexName = "idx" + Long.toString(System.nanoTime(), 36);

        JanusGraphManagement mgmt = graph.openManagement();
        JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Vertex.class)
            .addKey(mgmt.getPropertyKey("name"));
        for (int p = 0; p < Math.min(indexedProps, extraProps); p++) {
            builder.addKey(mgmt.getPropertyKey(propName(p)));
        }
        builder.buildMixedIndex(INDEX_BACKEND_NAME);
        mgmt.commit();

        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.REGISTER_INDEX).get();
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, indexName).status(SchemaStatus.REGISTERED).call();

        System.out.printf("Starting REINDEX of %s (threads=%d)...%n", indexName, threads);
        final long t0 = System.nanoTime();
        mgmt = graph.openManagement();
        ScanMetrics metrics = mgmt.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.REINDEX, threads).get();
        mgmt.commit();
        final long t1 = System.nanoTime();
        ManagementSystem.awaitGraphIndexStatus(graph, indexName).status(SchemaStatus.ENABLED).call();

        final double seconds = (t1 - t0) / 1e9;
        final long success = metrics.get(ScanMetrics.Metric.SUCCESS);
        final long failure = metrics.get(ScanMetrics.Metric.FAILURE);
        final long docUpdates = metrics.getCustom("doc-updates");
        final double perMin = size / seconds * 60d;
        System.out.printf(
            "RESULT index=%s size=%d threads=%d pageSize=%d scanPageSize=%d ranges=%d batch=%d ppl=%s "
                + "elapsed=%.1fs rate=%.0f vertices/min (%.0f/s) success=%d failure=%d docUpdates=%d%n",
            indexName, size, threads,
            Integer.getInteger("bench.pageSize", DEFAULT_PAGE_SIZE),
            Integer.getInteger("bench.scanPageSize", DEFAULT_SCAN_PAGE_SIZE),
            Integer.getInteger("bench.ranges", DEFAULT_RANGES),
            Integer.getInteger("bench.batchSize", DEFAULT_BATCH_SIZE),
            System.getProperty("bench.perPartitionLimit", "default"),
            seconds, perMin, size / seconds, success, failure, docUpdates);
        if (docUpdates != size) {
            // Every vertex carries indexed properties, so the doc count must equal the vertex count.
            // A mismatch means the scan lost or duplicated keys (gap/overlap) - or the keyspace holds
            // unexpected data.
            System.out.printf("WARNING: docUpdates=%d does not match expected %d - possible scan gap/overlap!%n",
                docUpdates, size);
        }
        return new Result(indexName, seconds, success, failure, docUpdates, size);
    }
}
