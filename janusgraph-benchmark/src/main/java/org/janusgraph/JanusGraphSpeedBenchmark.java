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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class JanusGraphSpeedBenchmark {

    @Param({ "1000", "10000", "100000" })
    long numberOfVertices;

    public JanusGraph graph;

    private static final String START_LABEL = "startVertex";
    private static final String END_LABEL = "endVertex";
    private static final String UID_PROP = "uid";

    @Setup
    public void setup() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.AUTO_TYPE, "none");
        config.set(GraphDatabaseConfiguration.SCHEMA_CONSTRAINTS, true);
        graph = JanusGraphFactory.open(config);
        JanusGraphManagement jgm = graph.openManagement();
        VertexLabel startVertex = jgm.makeVertexLabel(START_LABEL).make();
        PropertyKey uid = jgm.makePropertyKey(UID_PROP).dataType(Integer.class).make();
        jgm.buildIndex("byUid", Vertex.class).addKey(uid).indexOnly(startVertex).buildCompositeIndex();
        jgm.addProperties(startVertex, uid);
        VertexLabel endVertex = jgm.makeVertexLabel(END_LABEL).make();
        jgm.addProperties(endVertex, uid);
        EdgeLabel between = jgm.makeEdgeLabel("between").make();
        jgm.addConnection(between, startVertex, endVertex);

        jgm.commit();
        Vertex next = graph.traversal().addV(START_LABEL).property(UID_PROP, 1).next();

        for (int i = 0; i < numberOfVertices; i++) {
            graph.traversal()
                .addV(END_LABEL).property(UID_PROP, i).as("end")
                .addE("between").to("end").from(next).iterate();
        }
    }

    @Benchmark
    public void basicCount() {
        if (numberOfVertices != graph.traversal().V().has(START_LABEL, UID_PROP, 1).out().count().next())
            throw new AssertionError();
    }

    @Benchmark
    public void basicAddAndDelete(){
        for (int i = 0; i < numberOfVertices; i++) {
            GraphTraversalSource g = graph.traversal();
            g.addV(START_LABEL).property(UID_PROP, i+2).iterate();
        }
        for (int i = 0; i < numberOfVertices; i++) {
            GraphTraversalSource g = graph.traversal();
            g.V().has(START_LABEL, UID_PROP, i+2).iterate();
        }
    }

    @TearDown
    public void teardown() {
        graph.close();
    }

}
