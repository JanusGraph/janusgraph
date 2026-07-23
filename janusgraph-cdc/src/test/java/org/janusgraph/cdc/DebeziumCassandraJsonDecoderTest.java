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

package org.janusgraph.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.CdcElementChange;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebeziumCassandraJsonDecoderTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private StandardJanusGraph graph;
    private IDManager idManager;
    private DebeziumCassandraJsonDecoder decoder;

    @BeforeEach
    public void setUp() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        graph = (StandardJanusGraph) JanusGraphFactory.open(config.getConfiguration());
        idManager = graph.getIDManager();
        decoder = new DebeziumCassandraJsonDecoder(graph);
    }

    @AfterEach
    public void tearDown() {
        if (graph != null && graph.isOpen()) graph.close();
    }

    private byte[] bytesOf(StaticBuffer b) {
        return b.as(StaticBuffer.ARRAY_FACTORY);
    }

    /** Build a Debezium-Cassandra value JSON (schemas.enable=false). column/value may be null to omit. */
    private byte[] event(String table, String op, byte[] key, byte[] column1, byte[] value) {
        ObjectNode after = mapper.createObjectNode();
        after.set("key", cell(key));
        if (column1 != null) after.set("column1", cell(column1));
        if (value != null) after.set("value", cell(value));
        ObjectNode source = mapper.createObjectNode();
        source.put("keyspace", "janusgraph");
        source.put("table", table);
        ObjectNode payload = mapper.createObjectNode();
        payload.put("op", op);
        payload.set("source", source);
        payload.set("after", after);
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

    private ObjectNode cell(byte[] b) {
        ObjectNode c = mapper.createObjectNode();
        c.put("value", Base64.getEncoder().encodeToString(b));
        return c;
    }

    /**
     * Asserts the decoded changes are exactly one EDGE change with exactly this identity. Compared via
     * {@code toString()}: {@link RelationIdentifier#equals} looks only at relationId+typeId, but relation index
     * documents are keyed by the full four-part string INCLUDING both endpoint ids -- an out/in endpoint swap or a
     * non-canonical endpoint id (both of which target the wrong document) must fail here, and would pass an
     * {@code equals} comparison.
     */
    private void assertEdgeChange(RelationIdentifier expected, Collection<CdcElementChange> changes, String message) {
        assertEquals(1, changes.size(), message);
        CdcElementChange change = changes.iterator().next();
        assertEquals(ElementCategory.EDGE, change.getCategory(), message);
        assertEquals(expected.toString(), change.getElementId().toString(), message);
    }

    @Test
    public void decodesVertexFromPartitionKey() {
        JanusGraphVertex v = graph.addVertex();
        graph.tx().commit();
        Long vid = (Long) v.id();
        byte[] key = bytesOf(idManager.getKey(vid));

        Collection<CdcElementChange> changes = decoder.decode(null, event("edgestore", "d", key, null, null));

        assertEquals(1, changes.size());
        assertEquals(new CdcElementChange(ElementCategory.VERTEX, vid), changes.iterator().next());
    }

    @Test
    public void decodesDeleteFromBeforeImageWhenAfterIsNull() {
        // Standard Debezium delete envelope: "after" is null and the deleted row is in "before". The removal must be
        // captured (not dropped), otherwise the mixed index stays permanently stale for deletes.
        JanusGraphVertex v = graph.addVertex();
        graph.tx().commit();
        Long vid = (Long) v.id();
        byte[] key = bytesOf(idManager.getKey(vid));

        ObjectNode before = mapper.createObjectNode();
        before.set("key", cell(key));
        ObjectNode source = mapper.createObjectNode();
        source.put("table", "edgestore");
        ObjectNode payload = mapper.createObjectNode();
        payload.put("op", "d");
        payload.set("source", source);
        payload.putNull("after");
        payload.set("before", before);
        byte[] event = payload.toString().getBytes(StandardCharsets.UTF_8);

        Collection<CdcElementChange> changes = decoder.decode(null, event);
        assertEquals(1, changes.size());
        assertEquals(new CdcElementChange(ElementCategory.VERTEX, vid), changes.iterator().next());
    }

    @Test
    public void ignoresNonEdgestoreTable() {
        // Non-edgestore tables are filtered before the key is decoded, so the key content is irrelevant here.
        byte[] key = new byte[]{1, 2, 3};
        assertTrue(decoder.decode(null, event("graphindex", "i", key, null, null)).isEmpty());
    }

    @Test
    public void ignoresKafkaTombstone() {
        assertTrue(decoder.decode("k".getBytes(StandardCharsets.UTF_8), null).isEmpty());
    }

    @Test
    public void skipsRecordWithInvalidBase64() {
        // A corrupt / poison-pill record whose cell value is not valid Base64 must be skipped (empty result), not
        // throw -- otherwise the worker would rewind and retry the same undecodable record forever (no progress).
        ObjectNode badCell = mapper.createObjectNode();
        badCell.put("value", "!!! not base64 !!!");
        ObjectNode after = mapper.createObjectNode();
        after.set("key", badCell);
        ObjectNode source = mapper.createObjectNode();
        source.put("table", "edgestore");
        ObjectNode payload = mapper.createObjectNode();
        payload.set("source", source);
        payload.set("after", after);
        byte[] event = payload.toString().getBytes(StandardCharsets.UTF_8);

        assertTrue(decoder.decode(null, event).isEmpty(), "a record with a non-Base64 cell is skipped, not thrown");
    }

    @Test
    public void skipsRecordWithMalformedPartitionKey() {
        // Valid Base64, but the decoded bytes are not a valid JanusGraph key, so idManager.getKeyID throws. The
        // record must be skipped rather than propagate the exception (which would loop the worker on the same record).
        Collection<CdcElementChange> changes = decoder.decode(null, event("edgestore", "c", new byte[0], null, null));
        assertTrue(changes.isEmpty(), "a record whose partition key cannot be decoded to a vertex id is skipped");
    }

    @Test
    public void decodesVertexFromPropertyColumn() {
        JanusGraphVertex v = graph.addVertex("name", "alice");
        graph.tx().commit();
        Long vid = (Long) v.id();

        Entry propEntry = serializeFirstRelation(vid, null);
        byte[] key = bytesOf(idManager.getKey(vid));
        byte[] col = bytesOf(propEntry.getColumn());
        byte[] val = bytesOf(propEntry.getValue());

        Collection<CdcElementChange> changes = decoder.decode(null, event("edgestore", "i", key, col, val));
        // A property column reindexes the owning vertex AND emits a PROPERTY change (for property-element indexes).
        assertTrue(changes.contains(new CdcElementChange(ElementCategory.VERTEX, vid)));
        assertTrue(changes.stream().anyMatch(c -> c.getCategory() == ElementCategory.PROPERTY),
            "property column should also emit a PROPERTY change");
    }

    @Test
    public void decodesEdgeFromEdgeColumn() {
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        Edge e = a.addEdge("knows", b, "since", "2020");
        graph.tx().commit();
        Long aid = (Long) a.id();
        RelationIdentifier expected = (RelationIdentifier) e.id();

        Entry edgeEntry = serializeFirstRelation(aid, "knows");
        byte[] key = bytesOf(idManager.getKey(aid));
        byte[] col = bytesOf(edgeEntry.getColumn());
        byte[] val = bytesOf(edgeEntry.getValue());

        Collection<CdcElementChange> changes = decoder.decode(null, event("edgestore", "i", key, col, val));
        assertEdgeChange(expected, changes, "the OUT column must decode to the edge's full identity");
    }

    @Test
    public void decodesEdgeFromInDirectionColumn() {
        // Each edge is stored twice: an OUT column at the out-vertex and an IN column at the in-vertex. The IN column
        // must decode to the SAME RelationIdentifier (out/in assigned by the parsed direction, not the row owner).
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        Edge e = a.addEdge("knows", b, "since", "2021");
        graph.tx().commit();
        Long aid = (Long) a.id();
        Long bid = (Long) b.id();
        RelationIdentifier expected = (RelationIdentifier) e.id();

        Entry inEntry = serializeFirstRelation(aid, "knows", 1); // position 1 = the IN-vertex side of the edge
        byte[] key = bytesOf(idManager.getKey(bid));             // ...whose row owner is the IN vertex
        Collection<CdcElementChange> changes =
            decoder.decode(null, event("edgestore", "i", key, bytesOf(inEntry.getColumn()), bytesOf(inEntry.getValue())));

        assertEdgeChange(expected, changes, "the IN column must yield the same edge identity as the OUT column");
    }

    @Test
    public void decodesEdgeDeleteWithoutValueCell() {
        // A Cassandra delete tombstone carries no value bytes. For MULTI edges the relation id and other endpoint live
        // in the column, so the edge document removal must still be decodable from the column alone.
        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        Edge e = a.addEdge("knows", b, "since", "2022");
        graph.tx().commit();
        Long aid = (Long) a.id();
        RelationIdentifier expected = (RelationIdentifier) e.id();

        Entry edgeEntry = serializeFirstRelation(aid, "knows", 0);
        byte[] key = bytesOf(idManager.getKey(aid));
        Collection<CdcElementChange> changes =
            decoder.decode(null, event("edgestore", "d", key, bytesOf(edgeEntry.getColumn()), null)); // no value cell

        assertEdgeChange(expected, changes, "a MULTI-edge delete without a value cell must still identify the edge");
    }

    @Test
    public void decodesEdgeDeleteWithoutValueCellForSignatureLabel() {
        // Labels defined with a signature store those property values in the value region, which a delete tombstone
        // does not carry. A full relation parse would throw reading the signature and lose the edge identity; the
        // header-only parse must still recover it from the column alone.
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey weight = mgmt.makePropertyKey("weight").dataType(String.class).make();
        mgmt.makeEdgeLabel("rated").signature(weight).make();
        mgmt.commit();

        JanusGraphVertex a = graph.addVertex();
        JanusGraphVertex b = graph.addVertex();
        Edge e = a.addEdge("rated", b, "weight", "w1");
        graph.tx().commit();
        Long aid = (Long) a.id();
        RelationIdentifier expected = (RelationIdentifier) e.id();

        Entry edgeEntry = serializeFirstRelation(aid, "rated", 0);
        byte[] key = bytesOf(idManager.getKey(aid));
        Collection<CdcElementChange> changes =
            decoder.decode(null, event("edgestore", "d", key, bytesOf(edgeEntry.getColumn()), null)); // no value cell

        assertEdgeChange(expected, changes,
            "a signature-label edge delete without a value cell must still identify the edge");
    }

    @Test
    public void canonicalizesPartitionedVertexIdsFromPartitionCopyRows() {
        // Partitioned vertices have TWO id contracts, in opposite directions, and this test pins down both:
        //  - The vertex's own index document is keyed by the CANONICAL id, but its adjacency arrives from
        //    per-partition rows whose keys decode to partition-local ids -> VERTEX changes must canonicalize
        //    (a removal issued under a partition-local id would silently miss the document).
        //  - An EDGE of a partitioned vertex is identified by the PARTITION-REPRESENTATIVE id it was assigned to:
        //    that is what the user-facing edge id (and so the edge document id) embeds, and it is exactly the raw
        //    row/column id of the edge's stored copies -> RelationIdentifier endpoints must stay RAW; canonicalizing
        //    them would produce an identity that matches no document the synchronous path ever wrote.
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS, 8);
        StandardJanusGraph partitionedGraph = (StandardJanusGraph) JanusGraphFactory.open(config.getConfiguration());
        try {
            JanusGraphManagement mgmt = partitionedGraph.openManagement();
            mgmt.makeVertexLabel("hub").partition().make();
            mgmt.commit();
            JanusGraphVertex hub = partitionedGraph.addVertex("hub");
            JanusGraphVertex other = partitionedGraph.addVertex();
            Edge e = hub.addEdge("knows", other);
            partitionedGraph.tx().commit();
            Long hubId = (Long) hub.id(); // the canonical id, keying the hub's own vertex document
            RelationIdentifier edgeId = (RelationIdentifier) e.id();

            IDManager idm = partitionedGraph.getIDManager();
            long copyId = hubId;
            for (long p = 0; p < idm.getPartitionBound() && copyId == hubId; p++) {
                copyId = idm.getPartitionedVertexId(hubId, p);
            }
            assertNotEquals(hubId.longValue(), copyId, "expected a non-canonical representative id");
            DebeziumCassandraJsonDecoder partitionedDecoder = new DebeziumCassandraJsonDecoder(partitionedGraph);

            // Partition-level event from a partition-copy row: the VERTEX change must carry the canonical id.
            Collection<CdcElementChange> vertexChanges = partitionedDecoder.decode(null,
                event("edgestore", "d", bytesOf(idm.getKey(copyId)), null, null));
            assertEquals(Collections.singletonList(new CdcElementChange(ElementCategory.VERTEX, hubId)),
                new ArrayList<>(vertexChanges), "a partition-local vertex id must be canonicalized");

            // Edge column from the row the edge actually lives in (its out endpoint is the partition REPRESENTATIVE
            // the edge was assigned to, usually != the canonical id): the decoded identity must equal e.id() exactly,
            // i.e. the raw representative id must be preserved, not canonicalized.
            Entry edgeEntry = serializeFirstRelation(partitionedGraph, hubId, "knows", 0);
            byte[] edgeRowKey = bytesOf(idm.getKey(edgeId.getOutVertexId()));
            Collection<CdcElementChange> edgeChanges = partitionedDecoder.decode(null,
                event("edgestore", "i", edgeRowKey, bytesOf(edgeEntry.getColumn()), bytesOf(edgeEntry.getValue())));
            assertEdgeChange(edgeId, edgeChanges,
                "an edge of a partitioned vertex must keep its raw partition-representative endpoint id");
        } finally {
            partitionedGraph.close();
        }
    }

    @Test
    public void backendFailuresAreDetectedThroughCauseChains() {
        // Backend-caused parse failures must be rethrown (so the worker rewinds and redelivers) rather than treated
        // as poison-pill records; anything else falls back to the vertex-only reindex.
        assertTrue(DebeziumCassandraJsonDecoder.isBackendFailure(
            new JanusGraphException("wrap", new TemporaryBackendException("backend hiccup"))));
        assertTrue(DebeziumCassandraJsonDecoder.isBackendFailure(
            new RuntimeException(new JanusGraphException("nested", new TemporaryBackendException("deep")))));
        assertFalse(DebeziumCassandraJsonDecoder.isBackendFailure(new ArrayIndexOutOfBoundsException(3)));
        assertFalse(DebeziumCassandraJsonDecoder.isBackendFailure(new JanusGraphException("no backend cause")));
    }

    @Test
    public void propertyDeleteWithoutValueFallsBackToVertex() {
        // A property delete tombstone has no value bytes, and the property's relation id lives in the value region --
        // unrecoverable. The decoder must fall back to reindexing the owning vertex (never throw).
        JanusGraphVertex v = graph.addVertex("name", "bob");
        graph.tx().commit();
        Long vid = (Long) v.id();

        Entry propEntry = serializeFirstRelation(vid, null, 0);
        byte[] key = bytesOf(idManager.getKey(vid));
        Collection<CdcElementChange> changes =
            decoder.decode(null, event("edgestore", "d", key, bytesOf(propEntry.getColumn()), null)); // no value cell

        assertEquals(Collections.singletonList(new CdcElementChange(ElementCategory.VERTEX, vid)),
            new ArrayList<>(changes), "unparseable property delete falls back to the owning vertex");
    }

    /** Serializes the first matching relation of a vertex into an edgestore Entry (column+value). */
    private Entry serializeFirstRelation(Long vid, String edgeLabel) {
        return serializeFirstRelation(vid, edgeLabel, 0);
    }

    /** @param position 0 = the relation as stored at its out-vertex; 1 = as stored at its in-vertex (IN column). */
    private Entry serializeFirstRelation(Long vid, String edgeLabel, int position) {
        return serializeFirstRelation(graph, vid, edgeLabel, position);
    }

    private static Entry serializeFirstRelation(StandardJanusGraph g, Long vid, String edgeLabel, int position) {
        StandardJanusGraphTx tx = (StandardJanusGraphTx) g.newTransaction();
        try {
            Vertex v = tx.getVertex(vid);
            InternalRelation rel;
            if (edgeLabel != null) {
                rel = (InternalRelation) v.edges(Direction.OUT, edgeLabel).next();
            } else {
                VertexProperty<?> p = v.properties().next();
                rel = (InternalRelation) p;
            }
            return g.getEdgeSerializer().writeRelation(rel, position, tx);
        } finally {
            tx.rollback();
        }
    }
}
