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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.TransactionBuilder;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
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
public class LazyLoadBenchmark {

    @Param({"5000"})
    int verticesAmount;

    @Param({"true", "false"})
    boolean isLazyLoad;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());
        int itemsCount = 100;

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey idProp = mgmt.makePropertyKey("id").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey nameProp = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey detailsProp = mgmt.makePropertyKey("details").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey itemsProp = mgmt.makePropertyKey("items").dataType(String.class).cardinality(Cardinality.LIST).make();

        mgmt.buildIndex("nameIndex", Vertex.class).addKey(nameProp).buildCompositeIndex();

        mgmt.commit();

        for (int i = 0; i < verticesAmount; i++) {
            Vertex vertex = graph.addVertex("id", i);
            vertex.property("name", "name_test");
            vertex.property("details", "details_" + i);

            for (int j = 0; j < itemsCount; j++) {
                vertex.property("items", "item_" + j);
            }
        }

        graph.tx().commit();
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    @Benchmark
    public List<Integer> getProperties() {

        TransactionBuilder txBuilder = graph.buildTransaction();
        if (isLazyLoad) {
            txBuilder = txBuilder.lazyLoadRelations();
        } else {
            txBuilder = txBuilder.noLazyLoadRelations();
        }

        JanusGraphTransaction tx = txBuilder.start();
        List<? extends Property> properties = tx.traversal()
            .V()
            .has("name", "name_test")
            .properties()
            .toList();

        List<Integer> itemIds = properties.stream()
            .filter(p -> p.key().equals("id"))
            .map(Property::value)
            .map(id -> (Integer) id)
            .collect(Collectors.toList());

        tx.rollback();
        return itemIds;
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(LazyLoadBenchmark.class.getSimpleName())
            .warmupIterations(10)
            .measurementIterations(10)
            .build();
        new Runner(options).run();
    }

}
