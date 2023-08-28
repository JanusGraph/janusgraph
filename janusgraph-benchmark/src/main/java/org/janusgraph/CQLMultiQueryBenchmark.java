// Copyright 2022 JanusGraph Authors
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLMultiQueryBenchmark {
    @Param({"100", "500"})
    int fanoutFactor;

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

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.commit();

        Vertex mostInnerSingleVertex = graph.addVertex("name", "mostInner");
        Vertex mostOuter = graph.addVertex("name", "outer");
        for (int i = 0; i < fanoutFactor; i++) {
            Vertex otherV = graph.addVertex("name", "middle");
            mostOuter.addEdge("connects", otherV);
            for (int j = 0; j < fanoutFactor; j++) {
                Vertex innerV = graph.addVertex("name", "inner");
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
    public List<Object> getNeighborNames() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Object> names = tx.traversal().V().has("name", "outer").out().out().values("name").toList();
        tx.rollback();
        return names;
    }

    @Benchmark
    public List<Object> getNames() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Object> names = tx.traversal().V().has("name", "inner").values("name").toList();
        tx.rollback();
        return names;
    }

    @Benchmark
    public List<Map<String, Object>> getIdToOutVerticesProjection() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Map<String, Object>> idNameMap = tx.traversal().V().has("name", "middle")
            .project("a", "b").by(__.id()).by(__.out("connects")).toList();
        tx.rollback();
        return idNameMap;
    }

    @Benchmark
    public List<Vertex> getAllElementsTraversedFromOuterVertex() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "outer")
            .repeat(__.out("connects")).emit().toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getElementsWithUsingRepeatUntilSteps() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "outer")
            .repeat(__.out("connects")).until(__.not(__.out("connects"))).toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getElementsWithUsingEmitRepeatSteps() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "outer")
            .emit(__.in("connects")).repeat(__.out("connects")).toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getVerticesFilteredByAndStep() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "middle")
            .and(__.out("connects").count().is(P.gte(fanoutFactor)), __.hasId(P.neq(0))).toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getVerticesWithCoalesceUsage() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "middle")
            .coalesce(__.out("connects"), __.identity()).toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getVerticesWithDoubleUnion() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "middle")
            .union(__.union(__.out("connects"), __.identity()), __.identity()).toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Vertex> getVerticesFromMultiNestedRepeatStepStartingFromSingleVertex() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Vertex> result = tx.traversal().V().has("name", "mostInner")
            .emit().repeat(__.repeat(__.in("connects")).until(__.identity().loops().is(P.gt(0))))
            .until(__.identity().loops().is(P.gt(2)))
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<Long> getAdjacentVerticesLocalCounts() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<Long> result = tx.traversal().V().has("name", "inner")
            .local(__.in("connects").count())
            .toList();
        tx.rollback();
        return result;
    }

    @Benchmark
    public List<String> getLabels() {
        JanusGraphTransaction tx = graph.buildTransaction()
            .start();
        List<String> result = tx.traversal().V().has("name", "inner").label().toList();
        tx.rollback();
        return result;
    }
}
