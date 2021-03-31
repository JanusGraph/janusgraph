// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.query;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
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

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ADJUST_LIMIT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.HARD_MAX_LIMIT;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GraphCentricQueryBenchmark {
    @Param({"10000", "250000"})
    int size;

    @Param({"true", "false"})
    boolean useSmartLimit;

    @Param({"100000", "2147483647"})
    int hardMaxLimit;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.commit();

        final int batchSize = Math.min(10000, size);
        for (int i = 0; i < size / batchSize; i++) {
            for (int j = 0; j < batchSize; j++) {
                graph.addVertex("name", "value");
            }
            graph.tx().commit();
        }
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    @Benchmark
    public List<Vertex> getVertices() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .customOption(ConfigElement.getPath(ADJUST_LIMIT), useSmartLimit)
            .customOption(ConfigElement.getPath(HARD_MAX_LIMIT), hardMaxLimit)
            .start();
        List<Vertex> vertices = tx.traversal().V().has("name", "value").toList();
        tx.rollback();
        return vertices;
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(GraphCentricQueryBenchmark.class.getSimpleName())
            .warmupIterations(10)
            .measurementIterations(10)
            .build();
        new Runner(options).run();
    }

}
