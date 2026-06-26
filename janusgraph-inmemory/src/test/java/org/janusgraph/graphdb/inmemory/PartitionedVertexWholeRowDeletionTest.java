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
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.inmemory.WholeRowDeletionCapturingStoreManager;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that removing a partitioned vertex triggers whole-row deletion for
 * ALL physical representative rows (canonical + per-partition) in the edge store.
 *
 * <p>A partitioned vertex is stored once per partition that holds edges to/from it,
 * so its removal must whole-row-delete every such row, not only the canonical one.
 * The detection in {@code StandardJanusGraph.prepareCommitAddRelationMutations}
 * normalises each physical vertex-id to the canonical form before checking
 * {@code isVertexFullyRemoved}, which should match all representatives.
 */
public class PartitionedVertexWholeRowDeletionTest {

    private JanusGraph graph;

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

    /**
     * Opens a JanusGraph instance backed by the capturing in-memory store, with
     * whole-row deletion enabled and 8 partitions.
     * <p>
     * {@code ids.flush=false} forces JanusGraph to commit IDs up-front so that
     * vertices in each transaction are spread across distinct partitions —
     * the prerequisite for generating multiple physical rows for a partitioned vertex.
     */
    private JanusGraph openWithPartitions() {
        return JanusGraphFactory.build()
            .set("storage.backend", WholeRowDeletionCapturingStoreManager.class.getName())
            .set("storage.drop-whole-row-on-vertex-removal", true)
            .set("cluster.max-partitions", 8)
            // spread IDs across partitions from the first commit
            .set("ids.flush", false)
            // keep multiple concurrent partition blocks so edges land in different partitions
            .set("ids.num-partitions", 24)
            .open();
    }

    @Test
    public void partitionedVertexRemovalWholeRowDeletesAllRepresentatives() {
        graph = openWithPartitions();

        // ---- schema: a partitioned "hub" label and a plain edge label ----
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("hub").partition().make();
        mgmt.makeEdgeLabel("link").make();
        mgmt.commit();

        // ---- create the partitioned hub, then add leaves across MANY separate transactions ----
        // JanusGraph assigns a partition per transaction, so committing each leaf separately spreads
        // the leaves -- and therefore the hub's incident edges -- across multiple partitions. That is
        // what produces multiple physical representative rows for the partitioned hub. Creating
        // everything in a single transaction would cluster them into one partition (and make a
        // "more than one representative" assertion flaky).
        JanusGraphVertex hub = graph.addVertex("hub");
        graph.tx().commit();
        final long hubId = (long) hub.id();

        final IDManager idManager = ((StandardJanusGraph) graph).getIDManager();
        // confirm the hub is genuinely a partitioned vertex
        assertTrue(idManager.isPartitionedVertex(hubId),
            "hub must be assigned a partitioned-vertex id");

        for (int i = 0; i < 24; i++) {
            graph.traversal().V(hubId).next().addEdge("link", graph.addVertex());
            graph.tx().commit();
        }

        // The hub's physical representative rows (canonical + per-partition), as storage keys.
        final Set<StaticBuffer> representativeKeys =
            Arrays.stream(idManager.getPartitionedVertexRepresentatives(hubId))
                .mapToObj(repId -> idManager.getKey(repId))
                .collect(Collectors.toSet());

        // Which of the hub's representative rows actually received data during creation.
        final Set<StaticBuffer> hubRowsWithData =
            WholeRowDeletionCapturingStoreManager.CAPTURED.stream()
                .filter(c -> c.store.equals(EDGESTORE_NAME) && representativeKeys.contains(c.key))
                .map(c -> c.key)
                .collect(Collectors.toSet());

        // The multi-transaction setup must have spread the hub across more than one physical row,
        // otherwise this test would not actually exercise multi-representative deletion.
        assertTrue(hubRowsWithData.size() > 1,
            "test setup must spread the partitioned hub across multiple representative rows, got "
                + hubRowsWithData.size());

        // ---- reset captures so we only observe the deletion commit, then remove the hub ----
        WholeRowDeletionCapturingStoreManager.reset();
        graph.traversal().V(hubId).next().remove();
        graph.tx().commit();

        // ---- assertions ----
        final List<WholeRowDeletionCapturingStoreManager.Captured> edgeStoreWholeRows =
            WholeRowDeletionCapturingStoreManager.CAPTURED.stream()
                .filter(c -> c.store.equals(EDGESTORE_NAME) && c.wholeRow)
                .collect(Collectors.toList());
        final Set<StaticBuffer> deletedKeys = edgeStoreWholeRows.stream()
            .map(c -> c.key)
            .collect(Collectors.toSet());

        // Removal must whole-row delete EXACTLY the representative rows that held hub data: no row
        // missed (the regression this test guards) and no unrelated row touched.
        assertEquals(hubRowsWithData, deletedKeys,
            "whole-row deletion must cover exactly the partitioned hub's populated representative rows");

        // Each whole-row-deleted row carries 0 per-column deletions (the flag replaces them).
        assertTrue(edgeStoreWholeRows.stream().allMatch(c -> c.deletions == 0),
            "whole-row-deleted rows must have 0 per-column deletions");

        // Graph correctness: the hub is gone.
        assertFalse(graph.traversal().V(hubId).hasNext(),
            "partitioned hub vertex must be gone after removal");
    }
}
