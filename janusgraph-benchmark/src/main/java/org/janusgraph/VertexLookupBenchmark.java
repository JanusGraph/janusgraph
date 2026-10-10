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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Looks vertices up by id in a traversal of a new transaction each time, as a request does. With the lookup slices of
 * {@code query.fast-property-lookup-limit} (1000, the default) the existence check reads along what the step after the
 * lookup reads of each vertex anyway, the label or the label and all properties; with 0, the existence alone, and the
 * step reads what it reads itself, as before. The inmemory backend has no round trip, so the difference here is the
 * work of the reads themselves; on a remote backend each read saved is a round trip, see
 * {@link CQLVertexLookupBenchmark}.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class VertexLookupBenchmark {

    @Param({"0", "1000"})
    int lookupLimit;

    @Param({"20"})
    int properties;

    @Param({"1000"})
    int vertices;

    JanusGraph graph;
    Object[] ids;

    @Setup
    public void setUp() {
        graph = openGraph();
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("person").make();
        mgmt.makeEdgeLabel("knows").make();
        final String[] keys = new String[properties];
        for (int i = 0; i < properties; i++) {
            keys[i] = "p" + i;
            mgmt.makePropertyKey(keys[i]).dataType(String.class).make();
        }
        mgmt.commit();
        ids = new Object[vertices];
        JanusGraphVertex previous = null;
        for (int i = 0; i < vertices; i++) {
            final JanusGraphVertex v = graph.addVertex("person");
            for (String key : keys) {
                v.property(key, "value of " + key + " of vertex " + i);
            }
            if (previous != null) {
                previous.addEdge("knows", v);
            }
            previous = v;
            ids[i] = v.id();
            if (i % 100 == 99) {
                graph.tx().commit();
                previous = null;
            }
        }
        graph.tx().commit();
    }

    private Object anyId() {
        return ids[ThreadLocalRandom.current().nextInt(ids.length)];
    }

    private Object[] tenIds() {
        final Object[] ten = new Object[10];
        for (int i = 0; i < ten.length; i++) {
            ten[i] = anyId();
        }
        return ten;
    }

    //Runs the traversal in a new transaction, as a request does
    private <T> T inNewTransaction(Function<GraphTraversalSource, T> traversal) {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            return traversal.apply(tx.traversal());
        } finally {
            tx.rollback();
        }
    }

    /** Every property of a vertex, which the lookup reads along with the label. */
    @Benchmark
    public Map<Object, Object> valueMap() {
        return inNewTransaction(g -> g.V(anyId()).valueMap().next());
    }

    /** The label and every property of a vertex, as serializing a vertex reads them. */
    @Benchmark
    public Map<Object, Object> elementMap() {
        return inNewTransaction(g -> g.V(anyId()).elementMap().next());
    }

    /** The label of a vertex. */
    @Benchmark
    public String label() {
        return inNewTransaction(g -> g.V(anyId()).label().next());
    }

    /** A filter on a property, which reads all of them with query.fast-property, then another property. */
    @Benchmark
    public List<Object> hasThenValues() {
        return inNewTransaction(g -> g.V(anyId()).has("p0", P.neq("")).values("p1").toList());
    }

    /** One property, which the step reads on its own either way. */
    @Benchmark
    public List<Object> valuesOfOneKey() {
        return inNewTransaction(g -> g.V(anyId()).values("p0").toList());
    }

    /** The edges of a vertex, for which the lookup reads the existence alone either way. */
    @Benchmark
    public List<Vertex> out() {
        return inNewTransaction(g -> g.V(anyId()).out("knows").toList());
    }

    /** Ten vertices looked up together, with every property of each. */
    @Benchmark
    public List<Map<Object, Object>> tenValueMaps() {
        return inNewTransaction(g -> g.V(tenIds()).valueMap().toList());
    }

    /** Ten vertices looked up together, and their edges. */
    @Benchmark
    public List<Vertex> tenOut() {
        return inNewTransaction(g -> g.V(tenIds()).out("knows").toList());
    }

    JanusGraph openGraph() {
        //fast-property is the default, under which a has step reads all properties; set here so that the benchmark
        //doesn't depend on it
        return JanusGraphFactory.build().set("storage.backend", "inmemory")
            .set("query.fast-property", true)
            .set("query.fast-property-lookup-limit", lookupLimit).open();
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }
}
