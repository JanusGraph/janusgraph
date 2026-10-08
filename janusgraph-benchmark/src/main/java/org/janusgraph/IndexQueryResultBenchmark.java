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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Index queries in one transaction, whose index cache keeps the results it can, half of {@code cache.tx-cache-size}
 * ids in all (10,000 by default): the whole of a result larger than that, the result of a query under a small limit
 * before and after it, and many results of one vertex. The index is a composite index on the inmemory backend, whose
 * results are read whole, or a mixed index on Elasticsearch, whose results stream in pages of
 * {@code index.[X].max-result-set-size} hits.
 * <p>
 * The Elasticsearch node defaults to 127.0.0.1:9200; {@code -Dbench.es.host} and {@code -Dbench.es.port} point the
 * benchmark at another.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IndexQueryResultBenchmark {

    private static final String INDEX_BACKEND = "search";

    @Param({"composite", "mixed"})
    String index;

    @Param({"100000"})
    int vertices;

    JanusGraph graph;

    @Setup
    public void setUp() throws InterruptedException {
        final boolean mixed = "mixed".equals(index);
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        if (mixed) {
            config.set(GraphDatabaseConfiguration.INDEX_BACKEND, "elasticsearch", INDEX_BACKEND);
            config.set(GraphDatabaseConfiguration.INDEX_NAME, "indexqueryresultbench", INDEX_BACKEND);
            config.set(GraphDatabaseConfiguration.INDEX_HOSTS,
                new String[]{System.getProperty("bench.es.host", "127.0.0.1")}, INDEX_BACKEND);
            config.set(GraphDatabaseConfiguration.INDEX_PORT, Integer.getInteger("bench.es.port", 9200), INDEX_BACKEND);
            config.set(ElasticSearchIndex.NUMBER_OF_SHARDS, 1, INDEX_BACKEND);
            config.set(ElasticSearchIndex.NUMBER_OF_REPLICAS, 0, INDEX_BACKEND);
            //Pages of 10,000 hits, the most one request returns by default
            config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 10_000, INDEX_BACKEND);
        }
        graph = JanusGraphFactory.open(config.getConfiguration());
        final JanusGraphManagement management = graph.openManagement();
        final PropertyKey group = management.makePropertyKey("group").dataType(Integer.class).make();
        final PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
        if (mixed) {
            management.buildIndex("byGroupAndName", Vertex.class).addKey(group)
                .addKey(name, Mapping.STRING.asParameter()).buildMixedIndex(INDEX_BACKEND);
        } else {
            management.buildIndex("byGroup", Vertex.class).addKey(group).buildCompositeIndex();
            management.buildIndex("byName", Vertex.class).addKey(name).buildCompositeIndex();
        }
        management.commit();

        JanusGraphTransaction tx = graph.newTransaction();
        for (int i = 0; i < vertices; i++) {
            tx.addVertex("group", 0, "name", "v" + i);
            if (i % 10_000 == 9_999) {
                tx.commit();
                tx = graph.newTransaction();
            }
        }
        tx.commit();
        //Elasticsearch shows what it indexed after its next refresh
        while (mixed && graph.traversal().V().has("group", 0).count().next() < vertices) {
            graph.tx().rollback();
            Thread.sleep(200);
        }
        graph.tx().rollback();
    }

    @TearDown
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    private static void consume(GraphTraversal<Vertex, Object> ids, Blackhole blackhole) {
        while (ids.hasNext()) {
            blackhole.consume(ids.next());
        }
    }

    /**
     * Every vertex of a result larger than the transaction's index cache keeps.
     */
    @Benchmark
    public void wholeResult(Blackhole blackhole) {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            consume(tx.traversal().V().has("group", 0).id(), blackhole);
        } finally {
            tx.rollback();
        }
    }

    /**
     * The first ten vertices of the result, then all of them, then the first ten again: the transaction's index cache
     * answers the third query where the whole result didn't push the first one's out of it.
     */
    @Benchmark
    public void limitedResultAroundTheWholeResult(Blackhole blackhole) {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            final GraphTraversalSource g = tx.traversal();
            consume(g.V().has("group", 0).limit(10).id(), blackhole);
            consume(g.V().has("group", 0).id(), blackhole);
            consume(g.V().has("group", 0).limit(10).id(), blackhole);
        } finally {
            tx.rollback();
        }
    }

    /**
     * A hundred results of one vertex each, which the transaction's index cache keeps.
     */
    @Benchmark
    public void hundredResultsOfOneVertex(Blackhole blackhole) {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            final GraphTraversalSource g = tx.traversal();
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < 100; i++) {
                consume(g.V().has("name", "v" + random.nextInt(vertices)).id(), blackhole);
            }
        } finally {
            tx.rollback();
        }
    }
}
