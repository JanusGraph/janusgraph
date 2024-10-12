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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.server.JanusGraphServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class JanusGraphSerializerBaseIT {
    protected JanusGraphServer server;

    protected abstract GraphTraversalSource traversal();

    public void setUp(boolean useCustomId) {
        final String conf = useCustomId
            ? "src/test/resources/janusgraph-server-custom-id.yaml"
            : "src/test/resources/janusgraph-server-with-serializers.yaml";
        this.server = new JanusGraphServer(conf);
        server.start().join();
    }

    @AfterEach
    public void tearDown() {
        server.stop().join();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPropertiesWriteAndRead(boolean useCustomId) {
        setUp(useCustomId);
        GraphTraversalSource g = traversal();
        GraphTraversal<Vertex, Vertex> t1 = g.addV("person").property(single, "age", 29).property(single, "name", "marko");
        if (useCustomId) {
            t1.property(T.id, "marko");
        }
        t1.next();
        GraphTraversal<Vertex, Vertex> t2 = g.addV("person").property(single, "age", 27).property(single, "name", "vadas");
        if (useCustomId) {
            t2.property(T.id, "vadas");
        }
        t2.next();
        assertEquals(4, g.V().properties().toList().size());
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 1_000, 1_000_000, 10_000_000, 20_000_000, 21_000_000})
    public void testWriteLargeString(int stringLength) throws Exception {
        setUp(false);
        try (GraphTraversalSource g = traversal()) {
            GraphTraversal<Vertex, Vertex> t = g.addV("largeStringTestLabel");
            String largeStringPropertyKey = "largeString";
            String largeStringValue = RandomStringUtils.random(stringLength, 'a', 'b', 'c', 'd', 'e', 'f');
            Vertex initialVertex = t.property(largeStringPropertyKey, largeStringValue).next();

            Vertex recalledVertex = g.V().hasLabel("largeStringTestLabel").has(largeStringPropertyKey, largeStringValue).next();
            assertEquals(initialVertex.id(), recalledVertex.id());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGeoshapePointsWriteAndRead(boolean useCustomId, TestInfo testInfo) {
        setUp(useCustomId);
        GraphTraversalSource g = traversal();
        GraphTraversal<Vertex, Vertex> t = g.addV(testInfo.getDisplayName());
        if (useCustomId) {
            t.property(T.id, "customId");
        }
        t.property("geoshape", Geoshape.point(37.97, 23.72)).iterate();

        Geoshape test = (Geoshape) g.V().hasLabel(testInfo.getDisplayName()).values("geoshape").next();
        assertEquals(37.97, test.getPoint().getLatitude());
        assertEquals(23.72, test.getPoint().getLongitude());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testVertexSerialization(boolean useCustomId, TestInfo testInfo) {
        setUp(useCustomId);
        GraphTraversalSource g = traversal();

        GraphTraversal<Vertex, Vertex> t = g.addV(testInfo.getDisplayName());
        if (useCustomId) {
            t.property(T.id, 100000);
        }

        t.property("string", "test").iterate();

        Vertex vertex = g.V().hasLabel(testInfo.getDisplayName()).next();

        long id = (long) vertex.id();
        assertEquals(testInfo.getDisplayName(), g.V(id).label().next());
    }

    @Test
    public void testEdgeSerialization(TestInfo testInfo) {
        setUp(false);
        GraphTraversalSource g = traversal();
        RelationIdentifier inputId = (RelationIdentifier) g
            .addV(testInfo.getDisplayName()).as("from")
            .addV(testInfo.getDisplayName())
            .addE(testInfo.getDisplayName()).from("from")
            .id()
            .next();

        assertEquals(inputId, g.E(inputId).next().id());
        assertEquals(inputId, g.E(inputId).id().next());
    }

    @Test
    public void testEdgeSerializationWithCustomId(TestInfo testInfo) {
        setUp(true);
        GraphTraversalSource g = traversal();

        // both vertices have long ids
        RelationIdentifier id1 = (RelationIdentifier) g
            .addV(testInfo.getDisplayName()).as("from")
            .property(T.id, 10000)
            .addV(testInfo.getDisplayName())
            .property(T.id, 20000)
            .addE(testInfo.getDisplayName()).from("from")
            .id()
            .next();
        assertEquals(id1, g.E(id1).next().id());
        assertEquals(id1, g.E(id1).id().next());

        // both vertices have string ids
        RelationIdentifier id2 = (RelationIdentifier) g
            .addV(testInfo.getDisplayName()).as("from")
            .property(T.id, "jg_id:001")
            .addV(testInfo.getDisplayName())
            .property(T.id, "jg_id:002")
            .addE(testInfo.getDisplayName()).from("from")
            .id()
            .next();
        assertEquals(id2, g.E(id2).next().id());
        assertEquals(id2, g.E(id2).id().next());

        // one vertex has long id and another has string id
        RelationIdentifier id3 = (RelationIdentifier) g
            .addV(testInfo.getDisplayName()).as("from")
            .property(T.id, "30000")
            .addV(testInfo.getDisplayName())
            .property(T.id, 30000)
            .addE(testInfo.getDisplayName()).from("from")
            .id()
            .next();
        assertEquals(id3, g.E(id3).next().id());
        assertEquals(id3, g.E(id3).id().next());

        RelationIdentifier id4 = (RelationIdentifier) g
            .addV(testInfo.getDisplayName()).as("from")
            .property(T.id, 40000)
            .addV(testInfo.getDisplayName())
            .property(T.id, "40000")
            .addE(testInfo.getDisplayName()).from("from")
            .id()
            .next();
        assertEquals(id4, g.E(id4).next().id());
        assertEquals(id4, g.E(id4).id().next());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testJanusGraphTextPredicates(boolean useCustomId) {
        setUp(useCustomId);
        GraphTraversalSource g = traversal();

        GraphTraversal<Vertex, Vertex> t = g.addV("predicateTestLabel");
        if (useCustomId) {
            t.property(T.id, UUID.randomUUID().toString().replace("-", "@"));
        }

        t.property("name", "neptune").iterate();

        Vertex next = g.V().has("predicateTestLabel","name", Text.textPrefix("nep")).next();

        assertNotNull(next);

        next = g.V().has("predicateTestLabel","name", Text.textContains("neptune")).next();

        assertNotNull(next);
    }
}
