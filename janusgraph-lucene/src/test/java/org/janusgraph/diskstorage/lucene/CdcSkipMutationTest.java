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

package org.janusgraph.diskstorage.lucene;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.temporal.ChronoUnit;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the per-index CDC mutation modes on the synchronous commit path:
 * cdc-only (enabled + !synchronous) skips the synchronous mixed-index write, while
 * dual (enabled + synchronous) still writes it. The graph data is persisted in all cases.
 */
public class CdcSkipMutationTest {

    private static final String INDEX = "search";
    private static final String VERTEX_INDEX = "vsearch";

    @TempDir
    Path tempDir;

    private JanusGraph openGraph(boolean cdcEnabled, boolean synchronous) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(STORAGE_BACKEND, "berkeleyje");
        config.set(STORAGE_DIRECTORY, tempDir.resolve("bdb").toString());
        config.set(INDEX_BACKEND, "lucene", INDEX);
        config.set(INDEX_DIRECTORY, tempDir.resolve("lucene").toString(), INDEX);
        config.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, cdcEnabled, INDEX);
        config.set(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS, synchronous, INDEX);
        return JanusGraphFactory.open(config.getConfiguration());
    }

    private void createMixedIndex(JanusGraph g) throws InterruptedException {
        JanusGraphManagement mgmt = g.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex(VERTEX_INDEX, Vertex.class).addKey(name).buildMixedIndex(INDEX);
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(g, VERTEX_INDEX).status(SchemaStatus.ENABLED)
            .timeout(60, ChronoUnit.SECONDS).call();
    }

    @Test
    public void cdcOnlySkipsSynchronousMixedIndexWrite() throws Exception {
        JanusGraph g = openGraph(true, false);
        try {
            createMixedIndex(g);
            g.addVertex("name", "alice");
            g.tx().commit();
            // The vertex is persisted in the graph (full scan does not use the mixed index).
            assertEquals(1L, g.traversal().V().count().next().longValue());
            // But the mixed-index document was NOT written synchronously (cdc-only mode).
            assertEquals(0L, g.indexQuery(VERTEX_INDEX, "v.name:alice").vertexStream().count());
        } finally {
            g.close();
        }
    }

    @Test
    public void dualWritesMixedIndexSynchronously() throws Exception {
        JanusGraph g = openGraph(true, true);
        try {
            createMixedIndex(g);
            g.addVertex("name", "bob");
            g.tx().commit();
            assertEquals(1L, g.traversal().V().count().next().longValue());
            // dual mode: the mixed index IS written synchronously.
            assertEquals(1L, g.indexQuery(VERTEX_INDEX, "v.name:bob").vertexStream().count());
        } finally {
            g.close();
        }
    }

    @Test
    public void compositeIndexUnaffectedByCdcOnlyMode() throws Exception {
        // cdc-only filters mixed-index updates out at generation time (StandardJanusGraph.commitIndexAppliesToFilter);
        // composite indexes live in the primary storage backend and must never be caught by that filter.
        JanusGraph g = openGraph(true, false);
        try {
            JanusGraphManagement mgmt = g.openManagement();
            PropertyKey code = mgmt.makePropertyKey("code").dataType(String.class).make();
            mgmt.buildIndex("byCode", Vertex.class).addKey(code).buildCompositeIndex();
            mgmt.commit();
            ManagementSystem.awaitGraphIndexStatus(g, "byCode").status(SchemaStatus.ENABLED)
                .timeout(60, ChronoUnit.SECONDS).call();
            g.addVertex("code", "x1");
            g.tx().commit();
            assertEquals(1L, g.traversal().V().has("code", "x1").count().next().longValue(),
                "composite index lookups must keep working in cdc-only mode");
        } finally {
            g.close();
        }
    }
}
