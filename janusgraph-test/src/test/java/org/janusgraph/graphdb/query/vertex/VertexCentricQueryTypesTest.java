// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.query.vertex;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The types which {@link QueryUtil#getRelationTypes} tells of the conditions that the vertex-centric query builder
 * makes: the types of its labels and keys, and none for a query without them.
 */
public class VertexCentricQueryTypesTest {

    @Test
    public void shouldTellTheTypesOfTheConditionsOfTheQueryBuilder() {
        final JanusGraph graph = StorageSetup.getInMemoryGraph();
        try {
            final JanusGraphManagement management = graph.openManagement();
            management.makeEdgeLabel("member").make();
            management.makeEdgeLabel("owner").make();
            management.makePropertyKey("name").dataType(String.class).make();
            management.makePropertyKey("since").dataType(Integer.class).make();
            management.commit();
            final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
            try {
                final InternalVertex vertex = (InternalVertex) tx.addVertex();
                final InternalVertex other = (InternalVertex) tx.addVertex();
                final RelationType member = tx.getRelationType("member");
                final RelationType owner = tx.getRelationType("owner");
                final RelationType name = tx.getRelationType("name");
                assertEquals(Set.of(member), types(vertex, b -> b.labels("member"), RelationCategory.EDGE));
                assertEquals(Set.of(member), types(vertex, b -> b.labels("member").direction(Direction.OUT), RelationCategory.EDGE));
                assertEquals(Set.of(member), types(vertex, b -> b.labels("member").adjacent(other), RelationCategory.EDGE));
                assertEquals(Set.of(member, owner), types(vertex, b -> b.labels("member", "owner"), RelationCategory.EDGE));
                assertEquals(Set.of(member, owner),
                    types(vertex, b -> b.labels("member", "owner").has("since", Cmp.GREATER_THAN, 3), RelationCategory.EDGE));
                assertEquals(Set.of(member), types(vertex, b -> b.labels("member").has("since", 3), RelationCategory.EDGE));
                assertEquals(Set.of(name), types(vertex, b -> b.keys("name"), RelationCategory.PROPERTY));
                assertNull(types(vertex, b -> b, RelationCategory.EDGE));
                assertNull(types(vertex, b -> b, RelationCategory.RELATION));
                assertNull(types(vertex, b -> b.has("~label", "member"), RelationCategory.EDGE));
                assertNull(types(vertex, b -> b.has("since", 3), RelationCategory.EDGE));
            } finally {
                tx.rollback();
            }
        } finally {
            graph.close();
        }
    }

    private static Set<RelationType> types(InternalVertex vertex,
                                           Function<VertexCentricQueryBuilder, VertexCentricQueryBuilder> query,
                                           RelationCategory category) {
        final VertexCentricQueryBuilder builder = query.apply(new VertexCentricQueryBuilder(vertex));
        final BaseVertexCentricQuery base = builder.constructQuery(category);
        return QueryUtil.getRelationTypes(builder.constructQuery(vertex, base).getCondition());
    }
}
