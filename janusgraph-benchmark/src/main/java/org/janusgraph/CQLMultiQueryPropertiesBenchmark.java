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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
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
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLMultiQueryPropertiesBenchmark {
    @Param({"5000", "50000"})
    int verticesAmount;

    @Param({"true", "false"})
    boolean fastProperty;

    @Param({
        "all_properties",
        "required_properties_only",
        "none"})
    String propertiesBatchMode;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"cql");
        config.set(CQLConfigOptions.LOCAL_DATACENTER, "dc1");
        config.set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
        config.set(GraphDatabaseConfiguration.PROPERTIES_BATCH_MODE, propertiesBatchMode);
        config.set(GraphDatabaseConfiguration.PROPERTY_PREFETCHING, fastProperty);
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
    public List<? extends Object> getValueMap() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Map<Object, Object>> result = toVerticesTraversal(tx)
            .valueMap()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValueMapWithOptions() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Map<Object, Object>> result = toVerticesTraversal(tx)
            .valueMap().with(WithOptions.tokens, WithOptions.all)
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValueMapWithOptionsLimitedOne() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Map<Object, Object>> result = toVerticesTraversal(tx)
            .valueMap().with(WithOptions.tokens, WithOptions.all)
            .limit(1)
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValueMapWithOptionsLimitedBatchSizePlusOne() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Map<Object, Object>> result = toVerticesTraversal(tx)
            .valueMap()
            .with(WithOptions.tokens, WithOptions.all)
            .limit(GraphDatabaseConfiguration.LIMITED_BATCH_SIZE.getDefaultValue()+1)
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getPropertyMap() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .propertyMap()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getElementMap() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .elementMap()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValues() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .values()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getProperties() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .properties()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValueMapSingleProperty() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .valueMap("prop1")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getPropertyMapSingleProperty() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .propertyMap("prop1")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getElementMapSingleProperty() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .elementMap("prop1")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValuesSingleProperty() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .values("prop1")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getPropertiesSingleProperty() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .properties("prop1")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValuesMultipleProperties() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .values("prop1", "prop3", "prop5", "prop7", "prop9")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getValueMapMultipleProperties() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .valueMap("prop1", "prop3", "prop5", "prop7", "prop9")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<? extends Object> getElementMapMultipleProperties() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<? extends Object> result = toVerticesTraversal(tx)
            .elementMap("prop1", "prop3", "prop5", "prop7", "prop9")
            .toList();
        tx.rollback();
        return result;
    }

    private GraphTraversal<Vertex, Vertex> toVerticesTraversal(JanusGraphTransaction tx){
        return tx.traversal().V()
            .has("name", "testVertex");
    }
}
