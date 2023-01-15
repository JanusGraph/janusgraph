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

package org.janusgraph;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This benchmark evaluates performance of OLAP jobs that
 * can be run via the ManagementSystem interface, including:
 * 1) REINDEX
 * 2) DISCARD_INDEX
 *
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MgmtOlapJobBenchmark {
    @Param("10000")
    int size;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        return config.getConfiguration();
    }

    @Setup(Level.Iteration)
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());

        ((StandardJanusGraph) graph).getOpenTransactions().forEach(JanusGraphTransaction::rollback);

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameIndex").status(SchemaStatus.ENABLED).call();

        for (int j = 0; j < size; j++) {
            graph.addVertex("name", "value" + j, "alias", "value" + j);
        }
        graph.tx().commit();

        mgmt = graph.openManagement();
        mgmt.buildIndex("aliasIndex", Vertex.class).addKey(mgmt.getPropertyKey("alias")).buildCompositeIndex();
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "aliasIndex").status(SchemaStatus.ENABLED).call();

        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("nameIndex"), SchemaAction.DISABLE_INDEX).get();
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameIndex").status(SchemaStatus.DISABLED).call();
    }

    @Benchmark
    public void runReindex(Blackhole blackhole) throws ExecutionException, InterruptedException {
        JanusGraphManagement mgmt = graph.openManagement();
        blackhole.consume(mgmt.updateIndex(mgmt.getGraphIndex("aliasIndex"), SchemaAction.REINDEX).get());
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "aliasIndex").status(SchemaStatus.ENABLED).call();
    }

    @Benchmark
    public void runClearIndex(Blackhole blackhole) throws ExecutionException, InterruptedException {
        JanusGraphManagement mgmt = graph.openManagement();
        blackhole.consume(mgmt.updateIndex(mgmt.getGraphIndex("nameIndex"), SchemaAction.DISCARD_INDEX).get());
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameIndex").status(SchemaStatus.DISCARDED).call();
    }


    @TearDown(Level.Iteration)
    public void tearDown() {
        graph.close();
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(MgmtOlapJobBenchmark.class.getSimpleName())
            .warmupIterations(1)
            .measurementIterations(3)
            .build();
        new Runner(options).run();
    }

}
