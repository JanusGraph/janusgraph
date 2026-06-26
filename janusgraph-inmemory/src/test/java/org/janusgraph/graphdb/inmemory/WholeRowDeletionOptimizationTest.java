// Copyright 2024 JanusGraph Authors
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

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.inmemory.WholeRowDeletionCapturingStoreManager;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WholeRowDeletionOptimizationTest {

    private JanusGraph graph;

    private JanusGraph open(boolean enabled) {
        return JanusGraphFactory.build()
            .set("storage.backend", WholeRowDeletionCapturingStoreManager.class.getName())
            .set("storage.drop-whole-row-on-vertex-removal", enabled)
            .open();
    }

    @AfterEach
    public void close() {
        try {
            if (graph != null) {
                graph.close();
            }
        } finally {
            WholeRowDeletionCapturingStoreManager.reset();
        }
    }

    private long createSuperNode(int leaves) {
        JanusGraphVertex hub = graph.addVertex();
        for (int i = 0; i < leaves; i++) {
            hub.addEdge("link", graph.addVertex());
        }
        graph.tx().commit();
        return (long) hub.id();
    }

    @Test
    public void superNodeRemovalUsesWholeRowDeletion() {
        graph = open(true);
        long hubId = createSuperNode(50);
        StaticBuffer hubKey = ((StandardJanusGraph) graph).getIDManager().getKey(hubId);
        WholeRowDeletionCapturingStoreManager.reset();

        graph.traversal().V(hubId).next().remove();
        graph.tx().commit();

        long wholeRowEdgeStore = WholeRowDeletionCapturingStoreManager.CAPTURED.stream()
            .filter(c -> c.store.equals(EDGESTORE_NAME) && c.wholeRow).count();
        assertEquals(1, wholeRowEdgeStore, "exactly one edge-store row (the hub) deleted as whole-row");
        WholeRowDeletionCapturingStoreManager.CAPTURED.stream()
            .filter(c -> c.store.equals(EDGESTORE_NAME) && c.key.equals(hubKey))
            .forEach(c -> {
                assertTrue(c.wholeRow);
                assertEquals(0, c.deletions);
            });
        // graph state correct
        assertFalse(graph.traversal().V(hubId).hasNext());
    }

    @Test
    public void disabledFlagFallsBackToColumnDeletes() {
        graph = open(false);
        long hubId = createSuperNode(50);
        WholeRowDeletionCapturingStoreManager.reset();

        graph.traversal().V(hubId).next().remove();
        graph.tx().commit();

        assertEquals(0, WholeRowDeletionCapturingStoreManager.CAPTURED.stream()
            .filter(c -> c.wholeRow).count(), "no whole-row deletes when flag disabled");
        assertFalse(graph.traversal().V(hubId).hasNext());
    }

    @Test
    public void partialEdgeRemovalDoesNotWholeRowDelete() {
        graph = open(true);
        long hubId = createSuperNode(5);
        WholeRowDeletionCapturingStoreManager.reset();

        // remove ONE edge, keep the vertex
        JanusGraphVertex hub = (JanusGraphVertex) graph.traversal().V(hubId).next();
        hub.edges(org.apache.tinkerpop.gremlin.structure.Direction.OUT).next().remove();
        graph.tx().commit();

        assertEquals(0, WholeRowDeletionCapturingStoreManager.CAPTURED.stream()
            .filter(c -> c.wholeRow).count(), "surviving vertex must not be whole-row deleted");
        assertTrue(graph.traversal().V(hubId).hasNext());
    }
}
