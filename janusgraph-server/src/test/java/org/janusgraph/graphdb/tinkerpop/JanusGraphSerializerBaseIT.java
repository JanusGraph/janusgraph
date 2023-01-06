// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.server.JanusGraphServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class JanusGraphSerializerBaseIT {

    protected JanusGraphServer server;

    protected abstract GraphTraversalSource traversal();

    @BeforeEach
    public void setUp() {
        this.server = new JanusGraphServer("src/test/resources/janusgraph-server-with-serializers.yaml");
        server.start().join();
    }

    @AfterEach
    public void tearDown() {
        server.stop().join();
    }

    @Test
    public void testPropertiesWriteAndRead() {
        GraphTraversalSource g = traversal();
        g.addV("person").
            property(single, "age", 29).
            property(single, "name", "marko").
            addV("person").
            property(single, "age", 27).
            property(single, "name", "vadas").iterate();
        assertEquals(4, g.V().properties().toList().size());
    }

    @Test
    public void testGeoshapePointsWriteAndRead(TestInfo testInfo) {
        GraphTraversalSource g = traversal();
        g.addV(testInfo.getDisplayName()).property("geoshape", Geoshape.point(37.97, 23.72)).iterate();

        Geoshape test = (Geoshape) g.V().hasLabel(testInfo.getDisplayName()).values("geoshape").next();
        assertEquals(37.97, test.getPoint().getLatitude());
        assertEquals(23.72, test.getPoint().getLongitude());
    }

    @Test
    public void testVertexSerialization(TestInfo testInfo) {
        GraphTraversalSource g = traversal();

        g.addV(testInfo.getDisplayName()).property("string", "test").iterate();

        Vertex vertex = g.V().hasLabel(testInfo.getDisplayName()).next();

        long id = (long) vertex.id();
        assertEquals(testInfo.getDisplayName(), g.V(id).label().next());
    }

    @Test
    public void testEdgeSerialization(TestInfo testInfo) {
        GraphTraversalSource g = traversal();
        RelationIdentifier inputId = (RelationIdentifier) g
            .addV(testInfo.getDisplayName()).as("from")
            .addV(testInfo.getDisplayName())
            .addE(testInfo.getDisplayName()).from("from")
            .id()
            .next();

        Edge edge = g.E(inputId).next();

        RelationIdentifier outputId = (RelationIdentifier) edge.id();
        assertEquals(inputId, outputId);
    }

    @Test
    public void testRelationIdentifier(TestInfo testInfo) {
        GraphTraversalSource g = traversal();
        RelationIdentifier inputId = (RelationIdentifier) g
            .addV(testInfo.getDisplayName()).as("from")
            .addV(testInfo.getDisplayName())
            .addE(testInfo.getDisplayName()).from("from")
            .id()
            .next();

        RelationIdentifier outputId = (RelationIdentifier) g.E(inputId).id().next();

        assertEquals(inputId, outputId);
    }

    @Test
    @Disabled("JanusGraphPredicate serialization won't work any older version than 0.6.0.")
    public void testJanusGraphTextPredicates() {
        GraphTraversalSource g = traversal();
        g.addV("predicateTestLabel").property("name", "neptune").iterate();

        Vertex next = g.V().has("predicateTestLabel","name", Text.textPrefix("nep")).next();

        assertNotNull(next);

        next = g.V().has("predicateTestLabel","name", Text.textContains("neptune")).next();

        assertNotNull(next);
    }
}
