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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
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
 * Reads the properties of relations which have none: vertex properties without meta-properties and edges without
 * properties, which are the common case. Before JanusGraph 1.2.0 such a relation was parsed twice by its first read,
 * header-only when it was loaded and in full when its properties were read, and again on every later read, because an
 * empty property map was taken for a header-only parse.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RelationReparseBenchmark {

    @Param({"10", "100"})
    int properties;

    @Param({"100"})
    int edges;

    JanusGraph graph;
    Object vertexId;

    @Setup
    public void setUp() {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        JanusGraphManagement mgmt = graph.openManagement();
        for (int i = 0; i < properties; i++) {
            mgmt.makePropertyKey("p" + i).dataType(String.class).make();
        }
        mgmt.makePropertyKey("weight").dataType(Integer.class).make();
        mgmt.makeEdgeLabel("knows").make();
        mgmt.commit();
        JanusGraphVertex source = graph.addVertex();
        for (int i = 0; i < properties; i++) {
            source.property("p" + i, "value " + i);
        }
        for (int i = 0; i < edges; i++) {
            source.addEdge("knows", graph.addVertex());
        }
        graph.tx().commit();
        vertexId = source.id();
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    /**
     * Detaching a vertex with its properties reads the meta-properties of every vertex property, as Gremlin Server's
     * serializers also do when they write a vertex with its properties.
     */
    @Benchmark
    public Object detachVertexWithProperties() {
        JanusGraphTransaction tx = graph.newTransaction();
        try {
            return DetachedFactory.detach(tx.getVertex(vertexId), true);
        } finally {
            tx.rollback();
        }
    }

    @Benchmark
    public void metaPropertiesOfEveryVertexProperty(Blackhole bh) {
        JanusGraphTransaction tx = graph.newTransaction();
        try {
            Vertex vertex = tx.getVertex(vertexId);
            Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
            while (vertexProperties.hasNext()) {
                bh.consume(vertexProperties.next().properties().hasNext());
            }
        } finally {
            tx.rollback();
        }
    }

    @Benchmark
    public void propertyOfEveryEdge(Blackhole bh) {
        JanusGraphTransaction tx = graph.newTransaction();
        try {
            Vertex vertex = tx.getVertex(vertexId);
            Iterator<Edge> outgoing = vertex.edges(Direction.OUT);
            while (outgoing.hasNext()) {
                bh.consume(outgoing.next().property("weight").isPresent());
            }
        } finally {
            tx.rollback();
        }
    }

    /**
     * Reads the property of every edge twice, as a filter on the absence of a property followed by another read of
     * the edge's properties does, for example {@code outE().hasNot("weight").valueMap()}.
     */
    @Benchmark
    public void propertyOfEveryEdgeTwice(Blackhole bh) {
        JanusGraphTransaction tx = graph.newTransaction();
        try {
            Vertex vertex = tx.getVertex(vertexId);
            Iterator<Edge> outgoing = vertex.edges(Direction.OUT);
            while (outgoing.hasNext()) {
                Edge edge = outgoing.next();
                bh.consume(edge.property("weight").isPresent());
                bh.consume(edge.property("weight").isPresent());
            }
        } finally {
            tx.rollback();
        }
    }
}
