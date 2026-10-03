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
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Writes vertices, vertex properties and edge properties in a transaction on the inmemory backend. Every transaction
 * keeps the relations it adds in a container, and so does every vertex which gains a relation in it. Run it with
 * {@code -prof gc} to see the memory each transaction allocates.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TransactionWriteBenchmark {

    @Param({"1000"})
    int vertices;

    JanusGraph graph;
    Object[] vertexIds;
    int round;

    //A graph of its own for every iteration, so that the vertices added don't pile up
    @Setup(Level.Iteration)
    public void setUp() {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("person").make();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.makePropertyKey("weight").dataType(Integer.class).make();
        mgmt.makeEdgeLabel("knows").make();
        mgmt.commit();
        JanusGraphTransaction tx = graph.newTransaction();
        vertexIds = addPeople(tx);
        tx.commit();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        graph.close();
    }

    //People, each knowing the one added before
    private Object[] addPeople(JanusGraphTransaction tx) {
        Object[] ids = new Object[vertices];
        JanusGraphVertex previous = null;
        for (int i = 0; i < vertices; i++) {
            JanusGraphVertex person = tx.addVertex("person");
            person.property("name", "person " + i);
            person.property("age", i);
            if (previous != null) {
                person.addEdge("knows", previous, "weight", i);
            }
            ids[i] = person.id();
            previous = person;
        }
        return ids;
    }

    @Benchmark
    public void addVerticesWithPropertiesAndAnEdge() {
        JanusGraphTransaction tx = graph.newTransaction();
        addPeople(tx);
        tx.commit();
    }

    @Benchmark
    public void setAPropertyOfExistingVertices() {
        int value = ++round;
        JanusGraphTransaction tx = graph.newTransaction();
        for (Object id : vertexIds) {
            tx.getVertex(id).property("age", value);
        }
        tx.commit();
    }

    //Setting a property of an existing edge replaces the edge with one which keeps the id of the edge it replaces
    @Benchmark
    public void setAPropertyOfExistingEdges() {
        int value = ++round;
        JanusGraphTransaction tx = graph.newTransaction();
        for (Object id : vertexIds) {
            Iterator<Edge> known = tx.getVertex(id).edges(Direction.OUT, "knows");
            while (known.hasNext()) {
                known.next().property("weight", value);
            }
        }
        tx.commit();
    }
}
