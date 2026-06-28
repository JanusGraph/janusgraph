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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.CdcElementChange;
import org.janusgraph.graphdb.database.index.MixedIndexUpdateApplier;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies {@link MixedIndexUpdateApplier} reindexes-from-current-state for vertex and edge
 * adds/updates/removes, and is idempotent / order-independent. The graph is configured cdc-only so the
 * synchronous mixed-index write is skipped and the applier is the sole writer of the Lucene index.
 */
public class MixedIndexUpdateApplierTest {

    private static final String BACKING = "search";

    @TempDir
    Path tempDir;

    private JanusGraph graph;
    private MixedIndexUpdateApplier applier;

    @BeforeEach
    public void setUp() throws InterruptedException {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(STORAGE_BACKEND, "berkeleyje");
        config.set(STORAGE_DIRECTORY, tempDir.resolve("bdb").toString());
        config.set(INDEX_BACKEND, "lucene", BACKING);
        config.set(INDEX_DIRECTORY, tempDir.resolve("lucene").toString(), BACKING);
        config.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, BACKING);
        config.set(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS, false, BACKING); // cdc-only
        graph = JanusGraphFactory.open(config.getConfiguration());

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey since = mgmt.makePropertyKey("since").dataType(String.class).make();
        mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex(BACKING);
        mgmt.buildIndex("esearch", Edge.class).addKey(since).buildMixedIndex(BACKING);
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "vsearch").status(SchemaStatus.ENABLED)
            .timeout(60, ChronoUnit.SECONDS).call();
        ManagementSystem.awaitGraphIndexStatus(graph, "esearch").status(SchemaStatus.ENABLED)
            .timeout(60, ChronoUnit.SECONDS).call();

        applier = new MixedIndexUpdateApplier((StandardJanusGraph) graph, BACKING::equals);
    }

    @AfterEach
    public void tearDown() {
        if (graph != null && graph.isOpen()) {
            graph.close();
        }
    }

    private Long addVertex(String name) {
        JanusGraphVertex v = graph.addVertex("name", name);
        graph.tx().commit();
        return (Long) v.id();
    }

    private RelationIdentifier addEdge(String since) {
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        Edge e = a.addEdge("knows", b, "since", since);
        graph.tx().commit();
        return (RelationIdentifier) e.id();
    }

    private void applyVertex(Long... ids) {
        CdcElementChange[] changes = new CdcElementChange[ids.length];
        for (int i = 0; i < ids.length; i++) changes[i] = new CdcElementChange(ElementCategory.VERTEX, ids[i]);
        applier.apply(Arrays.asList(changes));
    }

    private void applyEdge(RelationIdentifier... ids) {
        CdcElementChange[] changes = new CdcElementChange[ids.length];
        for (int i = 0; i < ids.length; i++) changes[i] = new CdcElementChange(ElementCategory.EDGE, ids[i]);
        applier.apply(Arrays.asList(changes));
    }

    private long vertexHits(String name) {
        return graph.indexQuery("vsearch", "v.name:" + name).vertexStream().count();
    }

    private long edgeHits(String since) {
        return graph.indexQuery("esearch", "e.since:" + since).edgeStream().count();
    }

    private long propertyHits(String reading) {
        return graph.indexQuery("psearch", "p.reading:" + reading).propertyStream().count();
    }

    @Test
    public void reindexesAddedVertex() {
        Long vid = addVertex("alice");
        assertEquals(0L, vertexHits("alice"), "cdc-only: not written synchronously");
        applyVertex(vid);
        assertEquals(1L, vertexHits("alice"));
    }

    @Test
    public void reindexesUpdatedVertex() {
        Long vid = addVertex("alice");
        applyVertex(vid);
        assertEquals(1L, vertexHits("alice"));
        graph.traversal().V(vid).property("name", "alicia").iterate();
        graph.tx().commit();
        applyVertex(vid);
        assertEquals(1L, vertexHits("alicia"));
        assertEquals(0L, vertexHits("alice"));
    }

    @Test
    public void removesDeletedVertex() {
        Long vid = addVertex("alice");
        applyVertex(vid);
        assertEquals(1L, vertexHits("alice"));
        graph.traversal().V(vid).drop().iterate();
        graph.tx().commit();
        applyVertex(vid);
        assertEquals(0L, vertexHits("alice"));
    }

    @Test
    public void removesDocumentWhenIndexedPropertyRemovedButVertexRemains() {
        Long vid = addVertex("alice");
        applyVertex(vid);
        assertEquals(1L, vertexHits("alice"));
        // Remove the indexed property but keep the vertex: it now has no indexed field for vsearch, so its previously
        // written document must be removed (not left stale) when reindexed from current state.
        graph.traversal().V(vid).properties("name").drop().iterate();
        graph.tx().commit();
        applyVertex(vid);
        assertEquals(0L, vertexHits("alice"), "stale document removed once the vertex has no indexed field");
    }

    @Test
    public void reindexesAddedEdge() {
        RelationIdentifier eid = addEdge("2020");
        assertEquals(0L, edgeHits("2020"));
        applyEdge(eid);
        assertEquals(1L, edgeHits("2020"));
    }

    @Test
    public void removesDeletedEdge() {
        RelationIdentifier eid = addEdge("2020");
        applyEdge(eid);
        assertEquals(1L, edgeHits("2020"));
        graph.traversal().E(eid).drop().iterate();
        graph.tx().commit();
        applyEdge(eid);
        assertEquals(0L, edgeHits("2020"));
    }

    @Test
    public void reindexesPropertyElementIndex() throws InterruptedException {
        // A mixed index built on JanusGraphVertexProperty elements (a meta-property index) must also be refreshed
        // by the CDC applier; otherwise it would stay permanently stale in cdc-only mode (see issue review).
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("sensor").dataType(String.class).make();
        PropertyKey reading = mgmt.makePropertyKey("reading").dataType(String.class).make(); // meta-property key
        mgmt.buildIndex("psearch", JanusGraphVertexProperty.class).addKey(reading).buildMixedIndex(BACKING);
        mgmt.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "psearch").status(SchemaStatus.ENABLED)
            .timeout(60, ChronoUnit.SECONDS).call();
        // Rebuild the applier so it discovers the newly created property-element index.
        applier = new MixedIndexUpdateApplier((StandardJanusGraph) graph, BACKING::equals);

        JanusGraphVertex v = graph.addVertex();
        JanusGraphVertexProperty<?> p = (JanusGraphVertexProperty<?>) v.property("sensor", "s1");
        p.property("reading", "hot"); // meta-property indexed by psearch
        graph.tx().commit();
        RelationIdentifier pid = (RelationIdentifier) p.id();

        assertEquals(0L, propertyHits("hot"), "cdc-only: not written synchronously");
        applier.apply(Arrays.asList(new CdcElementChange(ElementCategory.PROPERTY, pid)));
        assertEquals(1L, propertyHits("hot"));
    }

    @Test
    public void removesEdgeDocumentWhenEndpointVertexRemoved() {
        // Removing a vertex removes its edges too, but the CDC event for the edge column arrives as an EDGE change
        // whose one endpoint no longer exists. Resolving it must yield "element gone -> remove the document", never
        // an NPE (which would wedge the CDC worker in an endless failing-retry loop).
        RelationIdentifier eid = addEdge("2033");
        applyEdge(eid);
        assertEquals(1L, edgeHits("2033"));
        graph.traversal().V(eid.getInVertexId()).drop().iterate(); // drops the in-vertex and with it the edge
        graph.tx().commit();
        applyEdge(eid);
        assertEquals(0L, edgeHits("2033"), "edge document removed although its in-vertex is gone");
    }

    @Test
    public void reindexesAndRemovesVertexWithCustomStringId() throws InterruptedException {
        // Graphs with custom string vertex ids: the CDC decoder yields String ids for them, and the applier must be
        // able to both reindex and remove such vertices (Long-only preconditions would poison-loop the worker).
        // inmemory backend: berkeleyje (an OrderedKeyValueStore) does not support custom vertex-id types.
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(STORAGE_BACKEND, "inmemory");
        config.set(INDEX_BACKEND, "lucene", BACKING);
        config.set(INDEX_DIRECTORY, tempDir.resolve("lucene-string").toString(), BACKING);
        config.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, BACKING);
        config.set(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS, false, BACKING);
        config.set(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID, true);
        config.set(GraphDatabaseConfiguration.ALLOW_CUSTOM_VERTEX_ID_TYPES, true);
        JanusGraph stringIdGraph = JanusGraphFactory.open(config.getConfiguration());
        try {
            JanusGraphManagement mgmt = stringIdGraph.openManagement();
            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
            mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex(BACKING);
            mgmt.commit();
            ManagementSystem.awaitGraphIndexStatus(stringIdGraph, "vsearch").status(SchemaStatus.ENABLED)
                .timeout(60, ChronoUnit.SECONDS).call();
            MixedIndexUpdateApplier stringIdApplier =
                new MixedIndexUpdateApplier((StandardJanusGraph) stringIdGraph, BACKING::equals);

            stringIdGraph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, "custom_alice", "name", "alice");
            stringIdGraph.tx().commit();
            stringIdApplier.apply(Arrays.asList(new CdcElementChange(ElementCategory.VERTEX, "custom_alice")));
            assertEquals(1L, stringIdGraph.indexQuery("vsearch", "v.name:alice").vertexStream().count());

            stringIdGraph.traversal().V("custom_alice").drop().iterate();
            stringIdGraph.tx().commit();
            stringIdApplier.apply(Arrays.asList(new CdcElementChange(ElementCategory.VERTEX, "custom_alice")));
            assertEquals(0L, stringIdGraph.indexQuery("vsearch", "v.name:alice").vertexStream().count());
        } finally {
            stringIdGraph.close();
        }
    }

    @Test
    public void idempotentAndOrderIndependent() {
        Long alice = addVertex("alice");
        Long bob = addVertex("bob");
        // shuffled + duplicated
        applyVertex(bob, alice, alice, bob);
        assertEquals(1L, vertexHits("alice"));
        assertEquals(1L, vertexHits("bob"));
        // applying again converges to the same state
        applyVertex(alice, bob);
        assertEquals(1L, vertexHits("alice"));
        assertEquals(1L, vertexHits("bob"));
    }
}
