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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Graph queries in a transaction which changed many of the vertices that an index holds: a mixed index on
 * Elasticsearch holds {@code kind}, {@code ckind} and {@code updatedAt} of 50,000 vertices, whose {@code updatedAt} is
 * 0 to 49,999, and a composite index holds {@code ckind} too. The transaction changes {@code changed} of them as
 * {@code change} says, and each operation runs one query ten times:
 * <ul>
 *     <li>{@code later}: the oldest get an {@code updatedAt} later than any the index holds;</li>
 *     <li>{@code earlier}: the newest get one earlier than any the index holds;</li>
 *     <li>{@code away}: the newest get another {@code kind} and {@code ckind}.</li>
 * </ul>
 * A changed vertex may belong in a result where the index doesn't have it, or not where the index has it, so each query
 * looks at the changed vertices which may match it, and leaves out what the index returns for them. Only the queries
 * are measured.
 * <p>
 * The Elasticsearch node defaults to 127.0.0.1:9200; {@code -Dbench.es.host} and {@code -Dbench.es.port} point the
 * benchmark at another.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
public class TransactionChangedVerticesBenchmark {

    @Param({"1000", "50000"})
    int changed;

    @Param({"later", "earlier", "away"})
    String change;

    @Param({"50000"})
    int vertices;

    JanusGraph graph;
    //By updatedAt, the oldest first
    List<Object> ids;

    @Setup
    public void setUp() throws InterruptedException {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND, "elasticsearch", "search");
        config.set(GraphDatabaseConfiguration.INDEX_NAME, "changedorderbench", "search");
        config.set(GraphDatabaseConfiguration.INDEX_HOSTS,
            new String[]{System.getProperty("bench.es.host", "127.0.0.1")}, "search");
        config.set(GraphDatabaseConfiguration.INDEX_PORT, Integer.getInteger("bench.es.port", 9200), "search");
        config.set(ElasticSearchIndex.NUMBER_OF_SHARDS, 1, "search");
        config.set(ElasticSearchIndex.NUMBER_OF_REPLICAS, 0, "search");
        graph = JanusGraphFactory.open(config.getConfiguration());
        final JanusGraphManagement management = graph.openManagement();
        final PropertyKey kind = management.makePropertyKey("kind").dataType(String.class).make();
        final PropertyKey ckind = management.makePropertyKey("ckind").dataType(String.class).make();
        final PropertyKey updatedAt = management.makePropertyKey("updatedAt").dataType(Long.class).make();
        management.buildIndex("byKindAndUpdatedAt", Vertex.class).addKey(kind, Mapping.STRING.asParameter())
            .addKey(ckind, Mapping.STRING.asParameter()).addKey(updatedAt).buildMixedIndex("search");
        management.buildIndex("byCkind", Vertex.class).addKey(ckind).buildCompositeIndex();
        management.commit();

        ids = new ArrayList<>(vertices);
        JanusGraphTransaction tx = graph.newTransaction();
        for (int i = 0; i < vertices; i++) {
            ids.add(tx.addVertex("kind", "k", "ckind", "c", "updatedAt", (long) i).id());
            if (i % 10_000 == 9_999) {
                tx.commit();
                tx = graph.newTransaction();
            }
        }
        tx.commit();
        //Elasticsearch shows what it indexed after its next refresh
        while (graph.traversal().V().has("kind", "k").count().next() < vertices) {
            graph.tx().rollback();
            Thread.sleep(200);
        }
        graph.tx().rollback();
    }

    @TearDown
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    /**
     * A transaction which changed {@code changed} of the vertices as {@code change} says.
     */
    @State(Scope.Thread)
    public static class ChangedTransaction {
        JanusGraphTransaction tx;

        @Setup(Level.Invocation)
        public void change(TransactionChangedVerticesBenchmark benchmark) {
            tx = benchmark.graph.newTransaction();
            final GraphTraversalSource g = tx.traversal();
            for (int i = 0; i < benchmark.changed; i++) {
                switch (benchmark.change) {
                    case "later":
                        g.V(benchmark.ids.get(i)).property("updatedAt", (long) (benchmark.vertices + i)).iterate();
                        break;
                    case "earlier":
                        g.V(benchmark.ids.get(benchmark.vertices - 1 - i)).property("updatedAt", -1L - i).iterate();
                        break;
                    case "away":
                        g.V(benchmark.ids.get(benchmark.vertices - 1 - i)).property("kind", "x").property("ckind", "x")
                            .iterate();
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown change: " + benchmark.change);
                }
            }
        }

        @TearDown(Level.Invocation)
        public void rollback() {
            tx.rollback();
        }
    }

    /**
     * The ten vertices of the kind with the latest {@code updatedAt}, which the index sorts.
     */
    @Benchmark
    public void latestTenOfTheKind(ChangedTransaction changes, Blackhole blackhole) {
        run(changes, g -> g.V().has("kind", "k").order().by("updatedAt", Order.desc).limit(10), blackhole);
    }

    /**
     * Any ten vertices of the kind with one of the ten latest {@code updatedAt} which the index holds or later.
     */
    @Benchmark
    public void tenOfTheKindUpdatedLately(ChangedTransaction changes, Blackhole blackhole) {
        run(changes, g -> g.V().has("kind", "k").has("updatedAt", P.gt((long) vertices - 11)).limit(10), blackhole);
    }

    /**
     * The same of {@code ckind}, which a composite index holds as well.
     */
    @Benchmark
    public void tenOfTheCompositeKindUpdatedLately(ChangedTransaction changes, Blackhole blackhole) {
        run(changes, g -> g.V().has("ckind", "c").has("updatedAt", P.gt((long) vertices - 11)).limit(10), blackhole);
    }

    private void run(ChangedTransaction changes, Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> query,
                     Blackhole blackhole) {
        for (int i = 0; i < 10; i++) {
            final GraphTraversal<Vertex, Object> ids = query.apply(changes.tx.traversal()).id();
            while (ids.hasNext()) {
                blackhole.consume(ids.next());
            }
        }
    }
}
