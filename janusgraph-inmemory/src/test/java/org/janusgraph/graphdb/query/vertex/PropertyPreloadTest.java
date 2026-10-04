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

import com.google.common.collect.Iterators;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A query for one property key preloads all properties of its vertex, unless the vertex holds them: the slice query it
 * checks for must be the one the preload caches.
 */
public class PropertyPreloadTest {

    private JanusGraph graph;
    private Object vertexId;

    @BeforeEach
    public void setUp() {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.commit();
        final JanusGraphVertex vertex = graph.addVertex();
        vertex.property("name", "alice");
        vertex.property("age", 30);
        graph.tx().commit();
        vertexId = vertex.id();
    }

    @AfterEach
    public void tearDown() {
        graph.close();
    }

    @Test
    public void thePreloadsSliceIsTheSerializersPropertiesSlice() {
        final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        try {
            assertEquals(tx.getEdgeSerializer().getQuery(RelationCategory.PROPERTY, false),
                VertexCentricQueryBuilder.ALL_PROPERTIES_SLICE);
        } finally {
            tx.rollback();
        }
    }

    @Test
    public void aVertexHoldsThePreloadsSliceOnceItsPropertiesWereRead() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            final InternalVertex vertex = (InternalVertex) tx.getVertex(vertexId);
            assertFalse(vertex.hasLoadedRelations(VertexCentricQueryBuilder.ALL_PROPERTIES_SLICE), "loaded before any read");

            Iterators.size(vertex.properties());
            assertTrue(vertex.hasLoadedRelations(VertexCentricQueryBuilder.ALL_PROPERTIES_SLICE), "not loaded by properties()");
        } finally {
            tx.rollback();
        }
    }

    @Test
    public void readingOnePropertyPreloadsThemAll() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            final InternalVertex vertex = (InternalVertex) tx.getVertex(vertexId);
            assertEquals("alice", vertex.value("name"));
            assertTrue(vertex.hasLoadedRelations(VertexCentricQueryBuilder.ALL_PROPERTIES_SLICE), "not preloaded by one read");
            assertEquals(30, (int) vertex.value("age"));
        } finally {
            tx.rollback();
        }
    }
}
