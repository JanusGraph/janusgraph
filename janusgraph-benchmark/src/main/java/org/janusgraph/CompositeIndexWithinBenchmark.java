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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Looks up many values of a composite index in memory, where reading the index costs little and what the query costs
 * on top of the reads shows. Before JanusGraph 1.2.0 building the condition of {@code within(values)} compared each
 * value with every one before it.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CompositeIndexWithinBenchmark {

    @Param({"1000", "10000"})
    int values;

    JanusGraph graph;
    List<Long> uids;

    @Setup
    public void setUp() {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        final JanusGraphManagement management = graph.openManagement();
        final PropertyKey uid = management.makePropertyKey("uid").dataType(Long.class).make();
        management.buildIndex("byUid", Vertex.class).addKey(uid).unique().buildCompositeIndex();
        management.commit();
        uids = new ArrayList<>(values);
        for (int i = 0; i < values; i++) {
            graph.addVertex("uid", (long) i);
            uids.add((long) i);
        }
        graph.tx().commit();
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    @Benchmark
    public List<Vertex> uniqueValues() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            return tx.traversal().V().has("uid", P.within(uids)).toList();
        } finally {
            tx.rollback();
        }
    }
}
