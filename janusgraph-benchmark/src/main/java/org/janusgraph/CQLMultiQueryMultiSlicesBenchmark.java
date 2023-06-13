// Copyright 2023 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryPropertiesStrategyMode;
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

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLMultiQueryMultiSlicesBenchmark {
    @Param({"5000", "50000"})
    int verticesAmount;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"cql");
        config.set(CQLConfigOptions.LOCAL_DATACENTER, "dc1");
        config.set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
        config.set(GraphDatabaseConfiguration.PROPERTIES_BATCH_MODE, MultiQueryPropertiesStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName());
        config.set(GraphDatabaseConfiguration.PROPERTY_PREFETCHING, false);
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());
        int propertiesAmount = 10;

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();

        for(int i=0;i<propertiesAmount;i++){
            mgmt.makePropertyKey("prop"+i).dataType(String.class).make();
        }

        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();

        mgmt.commit();

        for (int i = 0; i < verticesAmount; i++) {
            Vertex vertex = graph.addVertex("name", "testVertex");
            for(int j=0;j<propertiesAmount;j++){
                vertex.property("prop"+j,
                    "SomeTestPropertyValue "+j+" 0123456789 ABCDEFG");
            }
        }

        graph.tx().commit();
    }

    @TearDown
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    @Benchmark
    public List<? extends Object> getValuesMultiplePropertiesWithAllMultiQuerySlicesUnderMaxRequestsPerConnection() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .barrier(CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION.getDefaultValue() / 10 - 2)
            .values("prop0", "prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7", "prop8", "prop9")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValuesMultiplePropertiesWithUnlimitedBatch() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .barrier(Integer.MAX_VALUE)
            .values("prop0", "prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7", "prop8", "prop9")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValuesMultiplePropertiesWithSmallBatch() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .barrier(10)
            .values("prop0", "prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7", "prop8", "prop9")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValuesThreePropertiesWithAllMultiQuerySlicesUnderMaxRequestsPerConnection() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .barrier(100)
            .values("prop1", "prop3", "prop8")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValuesAllPropertiesWithAllMultiQuerySlicesUnderMaxRequestsPerConnection() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .barrier(CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION.getDefaultValue() / 10 - 2)
            .values()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValuesAllPropertiesWithUnlimitedBatch() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .barrier(Integer.MAX_VALUE)
            .values()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> vertexCentricPropertiesFetching() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Vertex> vertices = toVerticesTraversal(tx)
            .toList();
        List<Object> result = new ArrayList<>(vertices.size() * 10);
        for(Vertex vertex : vertices){
            vertex.properties("prop0", "prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7", "prop8", "prop9")
                .forEachRemaining(property -> result.add(property.value()));
        }
        tx.rollback();
        return result;
    }

    private GraphTraversal<Vertex, Vertex> toVerticesTraversal(JanusGraphTransaction tx){
        return tx.traversal().V()
            .has("name", "testVertex");
    }
}
