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

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Reads the properties of a vertex: single properties of a vertex whose properties a transaction has loaded already,
 * which measures the query machinery of a property access alone (resolving the key's name, building the query and
 * looking the result up in the vertex's relation cache), all of them in one query, and one property in a new
 * transaction, which loads them first. Each thread reads through a transaction of its own, as a transaction isn't
 * thread-safe; with several threads ({@code -t}) they share the graph, whose relation cache is synchronized.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PropertyAccessBenchmark {

    @Param({"20"})
    int properties;

    JanusGraph graph;
    Object vertexId;
    String[] keys;

    @Setup
    public void setUp() {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        final JanusGraphManagement mgmt = graph.openManagement();
        keys = new String[properties];
        for (int i = 0; i < properties; i++) {
            keys[i] = "p" + i;
            mgmt.makePropertyKey(keys[i]).dataType(String.class).make();
        }
        mgmt.commit();
        final JanusGraphVertex v = graph.addVertex();
        for (String key : keys) {
            v.property(key, "value of " + key);
        }
        graph.tx().commit();
        vertexId = v.id();
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    /** The transaction of one thread, and the vertex as that transaction holds it, its properties loaded. */
    @State(Scope.Thread)
    public static class ThreadTransaction {
        JanusGraphTransaction tx;
        Vertex vertex;

        @Setup
        public void setUp(PropertyAccessBenchmark shared) {
            tx = shared.graph.newTransaction();
            vertex = tx.getVertex(shared.vertexId);
            // load the properties once, as a transaction which has read the vertex has
            Iterators.size(vertex.properties());
        }

        @TearDown
        public void tearDown() {
            tx.rollback();
        }
    }

    /** One property of a vertex whose properties are loaded: the common access of an OLTP read. */
    @Benchmark
    public Object onePropertyOfALoadedVertex(ThreadTransaction t) {
        return t.vertex.value(keys[keys.length / 2]);
    }

    /** Every property of a loaded vertex, one access each. */
    @Benchmark
    public void everyPropertyOfALoadedVertex(ThreadTransaction t, Blackhole bh) {
        for (String key : keys) {
            bh.consume(t.vertex.value(key));
        }
    }

    /** All properties of a loaded vertex in one query. */
    @Benchmark
    public void allPropertiesOfALoadedVertex(ThreadTransaction t, Blackhole bh) {
        final Iterator<?> it = t.vertex.properties();
        while (it.hasNext()) {
            bh.consume(it.next());
        }
    }

    /** A property of a vertex read anew in a new transaction: the lookup plus the first load. */
    @Benchmark
    public Object onePropertyInANewTransaction() {
        final JanusGraphTransaction t = graph.newTransaction();
        try {
            return t.getVertex(vertexId).value(keys[keys.length / 2]);
        } finally {
            t.rollback();
        }
    }
}
