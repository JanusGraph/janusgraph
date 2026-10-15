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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Get-or-create loops in one transaction, committed at the end: look a vertex up by {@code uid} and add it with three
 * more properties where it is missing; a tenth of the lookups find a vertex the loop added before. {@code uid} is in a
 * composite index of its own, in one of {@code uid} and {@code kind} (the lookup then has both), or only in a mixed
 * index on Elasticsearch; or the loop only adds. The transaction is the thread's ({@code graph.tx()}) or one of its own
 * ({@code graph.newTransaction()}). Each iteration starts from an empty inmemory graph, and with several threads each
 * thread runs a loop of its own keys in a transaction of its own on the same graph.
 * <p>
 * The Elasticsearch node of the mixed index defaults to 127.0.0.1:9200; {@code -Dbench.es.host} and
 * {@code -Dbench.es.port} point the benchmark at another.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 8)
public class TransactionGetOrCreateBenchmark {

    @Param({"composite", "twoKeyComposite", "addsOnly", "mixed"})
    String index;

    @Param({"threadBound", "own"})
    String transaction;

    @Param({"20000"})
    int lookups;

    JanusGraph graph;

    @State(Scope.Thread)
    public static class Keys {
        final String prefix = UUID.randomUUID() + "-";
    }

    @Setup(Level.Iteration)
    public void setUp() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        //A lookup which no index answers fails rather than measuring a scan
        config.set(GraphDatabaseConfiguration.FORCE_INDEX_USAGE, true);
        if ("mixed".equals(index)) {
            config.set(GraphDatabaseConfiguration.INDEX_BACKEND, "elasticsearch", "search");
            config.set(GraphDatabaseConfiguration.INDEX_NAME, "getorcreatebench", "search");
            config.set(GraphDatabaseConfiguration.INDEX_HOSTS,
                new String[]{System.getProperty("bench.es.host", "127.0.0.1")}, "search");
            config.set(GraphDatabaseConfiguration.INDEX_PORT, Integer.getInteger("bench.es.port", 9200), "search");
            config.set(ElasticSearchIndex.NUMBER_OF_SHARDS, 1, "search");
            config.set(ElasticSearchIndex.NUMBER_OF_REPLICAS, 0, "search");
        }
        graph = JanusGraphFactory.open(config.getConfiguration());
        final JanusGraphManagement management = graph.openManagement();
        final PropertyKey uid = management.makePropertyKey("uid").dataType(String.class).make();
        final PropertyKey kind = management.makePropertyKey("kind").dataType(String.class).make();
        management.makePropertyKey("n").dataType(Integer.class).make();
        management.makePropertyKey("name").dataType(String.class).make();
        switch (index) {
            case "composite":
            case "addsOnly":
                management.buildIndex("byUid", Vertex.class).addKey(uid).buildCompositeIndex();
                break;
            case "twoKeyComposite":
                management.buildIndex("byUidAndKind", Vertex.class).addKey(uid).addKey(kind).buildCompositeIndex();
                break;
            case "mixed":
                management.buildIndex("byUidMixed", Vertex.class).addKey(uid, Mapping.STRING.asParameter())
                    .buildMixedIndex("search");
                break;
            default:
                throw new IllegalArgumentException(index);
        }
        management.commit();
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    @Benchmark
    public int getOrCreate(Keys keys) {
        final boolean own = "own".equals(transaction);
        final JanusGraphTransaction tx = own ? graph.newTransaction() : null;
        final GraphTraversalSource g = own ? tx.traversal() : graph.traversal();
        final int distinct = lookups * 9 / 10;
        int created = 0;
        for (int i = 0; i < lookups; i++) {
            final String uid = keys.prefix + (i % distinct);
            final boolean found;
            switch (index) {
                case "addsOnly":
                    found = false;
                    break;
                case "twoKeyComposite":
                    found = g.V().has("uid", uid).has("kind", "k").hasNext();
                    break;
                default:
                    found = g.V().has("uid", uid).hasNext();
            }
            if (!found) {
                g.addV().property("uid", uid).property("kind", "k").property("n", i).property("name", "vertex " + i)
                    .iterate();
                created++;
            }
        }
        if (own) {
            tx.commit();
        } else {
            graph.tx().commit();
        }
        return created;
    }
}
