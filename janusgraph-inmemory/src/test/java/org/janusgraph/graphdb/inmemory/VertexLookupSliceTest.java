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

package org.janusgraph.graphdb.inmemory;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.VertexLookup;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The slices which a lookup by id reads start with the existence marker: one ends after the label, the other covers the
 * properties too, and ends where the edges begin.
 */
public class VertexLookupSliceTest {

    private static StandardJanusGraph open(int lookupLimit) {
        return open(lookupLimit, false);
    }

    private static StandardJanusGraph open(int lookupLimit, boolean dbCache) {
        return (StandardJanusGraph) JanusGraphFactory.build().set("storage.backend", "inmemory")
            .set("query.fast-property-lookup-limit", lookupLimit).set("cache.db-cache", dbCache).open();
    }

    private static SliceQuery labelQuery(EdgeSerializer serializer) {
        return serializer.getQuery(BaseLabel.VertexLabelEdge, Direction.OUT, new EdgeSerializer.TypedInterval[0]);
    }

    //The query of a lookup is its slice, limited
    private static void assertLimitedSlice(StandardJanusGraph graph, VertexLookup lookup, int limit) {
        final SliceQuery slice = graph.vertexLookupSlice(lookup);
        assertFalse(slice.hasLimit());
        assertEquals(slice.getSliceStart(), graph.vertexLookupQuery(lookup).getSliceStart());
        assertEquals(slice.getSliceEnd(), graph.vertexLookupQuery(lookup).getSliceEnd());
        assertEquals(limit, graph.vertexLookupQuery(lookup).getLimit());
    }

    @Test
    public void theLabelSliceCoversExistenceAndLabelButNoProperty() throws Exception {
        try (StandardJanusGraph graph = open(1000)) {
            final EdgeSerializer serializer = graph.getEdgeSerializer();
            final SliceQuery slice = graph.vertexLookupSlice(VertexLookup.LABEL);
            assertTrue(slice.subsumes(graph.vertexExistenceQuery), "the existence marker");
            assertTrue(slice.subsumes(labelQuery(serializer)), "the label");
            assertFalse(slice.subsumes(serializer.getQuery(RelationCategory.PROPERTY, false)), "the properties");
            assertEquals(serializer.getQuery(RelationCategory.RELATION, true).getSliceEnd(), slice.getSliceEnd(),
                "ends with the system relations");
            assertLimitedSlice(graph, VertexLookup.LABEL, 1000);
        }
    }

    @Test
    public void thePropertiesSliceCoversExistenceLabelAndPropertiesButNoEdge() throws Exception {
        try (StandardJanusGraph graph = open(1000)) {
            final EdgeSerializer serializer = graph.getEdgeSerializer();
            final SliceQuery slice = graph.vertexLookupSlice(VertexLookup.LABEL_AND_PROPERTIES);
            assertTrue(slice.subsumes(graph.vertexExistenceQuery), "the existence marker");
            assertTrue(slice.subsumes(labelQuery(serializer)), "the label");
            assertTrue(slice.subsumes(serializer.getQuery(RelationCategory.PROPERTY, false)), "the properties");
            assertFalse(slice.subsumes(serializer.getQuery(RelationCategory.EDGE, false)), "the edges");
            assertEquals(serializer.getQuery(RelationCategory.EDGE, false).getSliceStart(), slice.getSliceEnd(),
                "ends where the edges begin");
            assertLimitedSlice(graph, VertexLookup.LABEL_AND_PROPERTIES, 1000);
        }
    }

    @Test
    public void theExistenceIsReadAloneWithoutASlice() throws Exception {
        try (StandardJanusGraph graph = open(1000)) {
            assertNull(graph.vertexLookupSlice(VertexLookup.EXISTENCE));
            assertSame(graph.vertexExistenceQuery, graph.vertexLookupQuery(VertexLookup.EXISTENCE));
        }
        //A limit of 0 turns the slices off, and so does cache.db-cache, which holds the slices the steps read
        for (StandardJanusGraph graph : new StandardJanusGraph[]{open(0), open(1000, true)}) {
            try {
                for (VertexLookup lookup : VertexLookup.values()) {
                    assertNull(graph.vertexLookupSlice(lookup), lookup.name());
                    assertSame(graph.vertexExistenceQuery, graph.vertexLookupQuery(lookup), lookup.name());
                    assertFalse(graph.vertexLookupIsComplete(EntryList.EMPTY_LIST, lookup), lookup.name());
                }
            } finally {
                graph.close();
            }
        }
    }

    @Test
    public void theExistenceMarkerLeadsTheRow() throws Exception {
        try (StandardJanusGraph graph = open(1000)) {
            final JanusGraphManagement mgmt = graph.openManagement();
            mgmt.makeVertexLabel("person").make();
            mgmt.makePropertyKey("name").dataType(String.class).make();
            mgmt.makePropertyKey("age").dataType(Integer.class).make();
            mgmt.commit();
            final JanusGraphVertex v = graph.addVertex("person");
            v.property("name", "john");
            v.property("age", 30);
            graph.tx().commit();

            final JanusGraphTransaction tx = graph.newTransaction();
            try {
                final StandardJanusGraphTx standardTx = (StandardJanusGraphTx) tx;
                final EntryList row = graph.edgeQuery(v.id(), graph.vertexLookupQuery(VertexLookup.LABEL_AND_PROPERTIES),
                    standardTx.getTxHandle());
                assertEquals(4, row.size(), "the marker, the label and two properties");
                assertTrue(graph.vertexExistsIn(row));
                assertTrue(graph.vertexLookupIsComplete(row, VertexLookup.LABEL_AND_PROPERTIES));
                final EntryList label = graph.edgeQuery(v.id(), graph.vertexLookupQuery(VertexLookup.LABEL),
                    standardTx.getTxHandle());
                assertEquals(2, label.size(), "the marker and the label");
                assertTrue(graph.vertexExistsIn(label));
                //The marker is the first cell, so a slice cut off at one cell shows it
                final SliceQuery slice = graph.vertexLookupSlice(VertexLookup.LABEL_AND_PROPERTIES);
                final SliceQuery oneCell = new SliceQuery(slice.getSliceStart(), slice.getSliceEnd()).setLimit(1);
                assertTrue(graph.vertexExistsIn(graph.edgeQuery(v.id(), oneCell, standardTx.getTxHandle())));
                assertFalse(graph.vertexExistsIn(EntryList.EMPTY_LIST));
            } finally {
                tx.rollback();
            }
        }
    }
}
