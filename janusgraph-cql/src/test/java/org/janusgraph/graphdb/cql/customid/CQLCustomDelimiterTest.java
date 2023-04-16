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

package org.janusgraph.graphdb.cql.customid;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_STRING_VERTEX_ID;
import static org.janusgraph.graphdb.relations.RelationIdentifier.JANUSGRAPH_RELATION_DELIMITER;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class CQLCustomDelimiterTest extends JanusGraphBaseTest {
    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    @Test
    public void testCustomRelationDelimiter() {
        System.setProperty(JANUSGRAPH_RELATION_DELIMITER, "@");
        clopen(option(ALLOW_SETTING_VERTEX_ID), true, option(ALLOW_STRING_VERTEX_ID), true);
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        Vertex v1 = graph.addVertex(T.id, id1, "prop", "val");
        Vertex v2 = graph.addVertex(T.id, id2, "prop", "val");
        graph.traversal().addE("link").from(v1).to(v2).property("count", 1).next();
        graph.tx().commit();

        assertEquals(2, graph.traversal().V().count().next());
        assertEquals(1, graph.traversal().E().count().next());
        Edge edge = graph.traversal().E().has("count", 1).next();
        assertEquals(id1, edge.outVertex().id());
        assertEquals(id2, edge.inVertex().id());
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName()).getConfiguration();
    }
}
