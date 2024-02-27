// Copyright 2024 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLMultiQueryDropBenchmark {

    @Param({"5000"})
    int verticesAmount;

    @Param({"true", "false"})
    boolean isMultiDrop;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"cql");
        config.set(CQLConfigOptions.LOCAL_DATACENTER, "dc1");
        config.set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());

        boolean alreadyAdded = true;
        JanusGraphManagement mgmt = graph.openManagement();
        if (!mgmt.containsPropertyKey("id")) {
            mgmt.makePropertyKey("id").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
            PropertyKey nameProp = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("details").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.buildIndex("nameIndex", Vertex.class).addKey(nameProp).buildCompositeIndex();

            alreadyAdded = false;
        }

        mgmt.commit();

        if (!alreadyAdded) {
            addVertices();
        }
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }
    @Benchmark
    public Integer dropVertices() {

        JanusGraphTransaction tx = graph.buildTransaction().start();
        Integer dropCount = 0;
        if (isMultiDrop) {

            List<JanusGraphVertex> vertices = tx.traversal()
                .V()
                .has("name", "name_test")
                .toList()
                .stream()
                .map(v -> (JanusGraphVertex) v)
                .collect(Collectors.toList());

            dropCount = tx.multiQuery(vertices).drop();
        } else {
            dropCount = tx.traversal()
                .V()
                .has("name", "name_test")
                .drop()
                .toList()
                .size();
        }

        tx.rollback();
        return dropCount;
    }

    private void addVertices() {
        for (int i = 0; i < verticesAmount; i++) {
            Vertex vertex = graph.addVertex("id", i);
            vertex.property("name", "name_test");
            vertex.property("details", "details_" + i);
        }

        graph.tx().commit();
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(CQLMultiQueryDropBenchmark.class.getSimpleName())
            .warmupIterations(3)
            .measurementIterations(10)
            .build();
        new Runner(options).run();
    }

}
