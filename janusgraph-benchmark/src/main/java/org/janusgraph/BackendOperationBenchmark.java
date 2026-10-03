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

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Every read of a transaction and every write of its commit runs through {@link BackendOperation}, which reattempts
 * temporary failures. Measures that for operations which succeed at once, as nearly all do: on their own, on one
 * thread and on eight, and as part of reading a property of 1,000 vertices on the inmemory backend, two operations per
 * vertex. {@link BenchmarkRunner} leaves it out of its default run, which takes ms/op only.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BackendOperationBenchmark {

    private static final Duration MAX_TIME = Duration.ofSeconds(10);

    @State(Scope.Benchmark)
    public static class Operation {
        final Callable<Integer> succeeding = () -> 1;
    }

    @Benchmark
    @Threads(1)
    public Integer succeedingOperation(Operation operation) throws BackendException {
        return BackendOperation.executeDirect(operation.succeeding, MAX_TIME);
    }

    @Benchmark
    @Threads(8)
    public Integer succeedingOperationOnEightThreads(Operation operation) throws BackendException {
        return BackendOperation.executeDirect(operation.succeeding, MAX_TIME);
    }

    @State(Scope.Benchmark)
    public static class Graph {
        JanusGraph graph;
        Object[] vertexIds = new Object[1_000];

        @Setup
        public void setUp() {
            graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
            JanusGraphManagement mgmt = graph.openManagement();
            mgmt.makePropertyKey("name").dataType(String.class).make();
            mgmt.commit();
            JanusGraphTransaction tx = graph.newTransaction();
            for (int i = 0; i < vertexIds.length; i++) {
                JanusGraphVertex vertex = tx.addVertex();
                vertex.property("name", "vertex " + i);
                vertexIds[i] = vertex.id();
            }
            tx.commit();
        }

        @TearDown
        public void tearDown() {
            graph.close();
        }
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void readAPropertyOfVertices(Graph graph, Blackhole bh) {
        readNames(graph, bh);
    }

    @Benchmark
    @Threads(8)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void readAPropertyOfVerticesOnEightThreads(Graph graph, Blackhole bh) {
        readNames(graph, bh);
    }

    private static void readNames(Graph graph, Blackhole bh) {
        JanusGraphTransaction tx = graph.graph.newTransaction();
        try {
            for (Object id : graph.vertexIds) {
                bh.consume(tx.getVertex(id).value("name"));
            }
        } finally {
            tx.rollback();
        }
    }
}
