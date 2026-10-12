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
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
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
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.concurrent.TimeUnit;

/**
 * Queries of a vertex which gained many edges in the open transaction: {@code edges} edges of one label and ten of
 * another, and a property. The transaction is bound to a thread ({@code threadBound}, as {@code graph.tx()} is) or one
 * which threads may share ({@code shared}, as {@code graph.newTransaction()} is), whose vertices keep their relations
 * in a container safe for concurrent use; with several benchmark threads, all of them query the vertex in the shared
 * one, each through a traversal of its own. A {@code threadBound} transaction is queried by one thread only. The graph
 * is an inmemory one, so the queries read the transaction's own relations mostly.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
public class NewEdgesQueryBenchmark {

    @Param({"100", "10000"})
    int edges;

    @Param({"threadBound", "shared"})
    String transaction;

    JanusGraph graph;
    JanusGraphTransaction tx;
    GraphTraversalSource g;
    Object hub;

    @Setup(Level.Trial)
    public void setUp(BenchmarkParams params) {
        if ("threadBound".equals(transaction) && params.getThreads() > 1) {
            throw new IllegalArgumentException("A transaction bound to a thread is queried by one thread only: "
                + "run transaction=threadBound with -t 1");
        }
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        graph = JanusGraphFactory.open(config.getConfiguration());
        final JanusGraphManagement management = graph.openManagement();
        management.makeEdgeLabel("member").make();
        management.makeEdgeLabel("owner").make();
        management.makePropertyKey("name").dataType(String.class).make();
        management.commit();

        tx = "shared".equals(transaction) ? graph.newTransaction() : ((StandardJanusGraph) graph).newThreadBoundTransaction();
        final JanusGraphVertex vertex = tx.addVertex("name", "hub");
        for (int i = 0; i < edges; i++) {
            vertex.addEdge("member", tx.addVertex());
        }
        for (int i = 0; i < 10; i++) {
            vertex.addEdge("owner", tx.addVertex());
        }
        hub = vertex.id();
        g = tx.traversal();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        tx.rollback();
        graph.close();
    }

    @Benchmark
    public long edgesOfTheOtherLabel() {
        return g.V(hub).outE("owner").count().next();
    }

    @Benchmark
    public Object property() {
        return g.V(hub).values("name").next();
    }

    @Benchmark
    public long edgesOfTheLabelWithMany() {
        return g.V(hub).outE("member").count().next();
    }
}
