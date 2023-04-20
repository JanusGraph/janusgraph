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

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLMultiQueryHasStepBenchmark {
    @Param({"100", "500"})
    int fanoutFactor;

    @Param({"true", "false"})
    boolean fastProperty;

    @Param({
        "all_properties",
        "required_properties_only",
        "required_and_next_properties",
        "required_and_next_properties_or_all"})
    String hasStepBatchMode;

    JanusGraph graph;

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"cql");
        config.set(CQLConfigOptions.LOCAL_DATACENTER, "dc1");
        config.set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
        config.set(GraphDatabaseConfiguration.HAS_STEP_BATCH_MODE, hasStepBatchMode);
        config.set(GraphDatabaseConfiguration.PROPERTY_PREFETCHING, fastProperty);
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws Exception {
        graph = JanusGraphFactory.open(getConfiguration());
        int additionalPropertiesAmount = 5;

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        for(int i=0;i<additionalPropertiesAmount;i++){
            mgmt.makePropertyKey("prop"+i).dataType(String.class).make();
        }

        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.commit();

        Vertex mostInnerSingleVertex = graph.addVertex("name", "mostInner");
        Vertex mostOuter = graph.addVertex("name", "outer");
        for (int i = 0; i < fanoutFactor; i++) {
            Vertex otherV = graph.addVertex("name", "middle");
            mostOuter.addEdge("connects", otherV);
            for (int j = 0; j < fanoutFactor; j++) {
                Vertex innerV = graph.addVertex("name", "inner");
                for(int k=0;k<additionalPropertiesAmount;k++){
                    innerV.property("prop"+k,
                        "SomeTestPropertyValue "+k+" 0123456789 ABCDEFG");
                }
                otherV.addEdge("connects", innerV);
                innerV.addEdge("connects", mostInnerSingleVertex);
            }
            graph.tx().commit();
        }
    }

    @TearDown
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    @Benchmark
    public List<Vertex> getVerticesFilteredByHasStep() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "mostInner")
            .in("connects").has("name", "inner")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getVerticesFilteredByHasStepWithNonHasStepAfterOut() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "mostInner")
            .in("connects").map(Traverser::get).has("name", "inner")
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getVerticesFilteredByHasStepInParentStep() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "mostInner")
            .in("connects").<Vertex>union(__.has("name", "inner"))
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Object> getAllPropertiesOfVerticesFilteredByHasStep() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Object> result = tx.traversal().V().has("name", "mostInner")
            .in("connects").has("name", "inner").values()
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Object> getSpecificPropertiesOfVerticesFilteredByHasStep() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Object> result = tx.traversal().V().has("name", "mostInner")
            .in("connects").has("name", "inner").values("prop1", "prop2")
            .toList();
        tx.rollback();
        return result;
    }
}
