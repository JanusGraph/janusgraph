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

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VertexCentricQueryBenchmark {
    @Param({"10000", "100000"})
    int size;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());

        final int batchSize = Math.min(10000, size);
        for (int i = 0; i < size / batchSize; i++) {
            for (int j = 0; j < batchSize; j++) {
                graph.addVertex("key1", "value1", "key2", "value2", "key3", "value3");
            }
            graph.tx().commit();
        }
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    @Benchmark
    public List<Object> getValues() {
        List<Object> values = graph.traversal().V().values().toList();
        graph.traversal().tx().rollback();
        return values;
    }

    @Benchmark
    public TraversalMetrics getValuesProfile() {
        TraversalMetrics profile = graph.traversal().V().values().profile().next();
        graph.traversal().tx().rollback();
        return profile;
    }

    @Benchmark
    public List<Map<Object, Object>> getValueMap() {
        List<Map<Object, Object>> valueMaps = graph.traversal().V().valueMap().toList();
        graph.traversal().tx().rollback();
        return valueMaps;
    }

    @Benchmark
    public TraversalMetrics getValueMapProfile() {
        TraversalMetrics profile = graph.traversal().V().valueMap().profile().next();
        graph.traversal().tx().rollback();
        return profile;
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(VertexCentricQueryBenchmark.class.getSimpleName())
            .warmupIterations(3)
            .measurementIterations(5)
            .build();
        new Runner(options).run();
    }

}
