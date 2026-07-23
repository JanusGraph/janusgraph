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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.RelationType;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.CdcElementChange;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Decodes Debezium's Apache Cassandra change events for JanusGraph's {@code edgestore} table into
 * {@link CdcElementChange}s.
 *
 * <p>The event's partition key is the JanusGraph edgestore row key, which decodes to the owning vertex id via
 * {@link IDManager#getKeyID(StaticBuffer)}. The clustering column ({@code column1}) plus the cell {@code value} encode a
 * single relation (property or edge); its header is parsed with JanusGraph's own
 * {@link org.janusgraph.graphdb.database.EdgeSerializer} so we can tell whether the vertex (property changed) or an
 * edge (and its {@link org.janusgraph.graphdb.relations.RelationIdentifier}) needs reindexing. Blob columns are
 * Base64-encoded by Debezium's JSON converter.</p>
 *
 * <p>This decoder is graph-aware (it needs JanusGraph's id manager and edge serializer) but performs only reads. It
 * tolerates both {@code value.converter.schemas.enable=true} (fields nested under {@code payload}) and {@code false}.</p>
 */
public class DebeziumCassandraJsonDecoder implements CdcEventDecoder {

    private static final Logger log = LoggerFactory.getLogger(DebeziumCassandraJsonDecoder.class);

    private static final JsonPointer SOURCE_TABLE = JsonPointer.compile("/source/table");

    private final StandardJanusGraph graph;
    private final IDManager idManager;
    private final ObjectMapper mapper = new ObjectMapper();

    public DebeziumCassandraJsonDecoder(StandardJanusGraph graph) {
        this.graph = graph;
        this.idManager = graph.getIDManager();
    }

    @Override
    public Collection<CdcElementChange> decode(byte[] messageKey, byte[] messageValue) {
        if (messageValue == null) {
            return Collections.emptyList(); // Kafka tombstone
        }
        final JsonNode root;
        try {
            root = mapper.readTree(messageValue);
        } catch (IOException e) {
            log.warn("Skipping CDC record whose value is not valid JSON", e);
            return Collections.emptyList();
        }
        if (root == null || root.isNull()) {
            return Collections.emptyList();
        }
        // schemas.enable=true wraps everything under "payload"; schemas.enable=false is flat.
        final JsonNode payload = root.hasNonNull("payload") ? root.get("payload") : root;
        if (!Backend.EDGESTORE_NAME.equals(payload.at(SOURCE_TABLE).asText(""))) {
            return Collections.emptyList();
        }
        // Use the "after" row image, falling back to "before": a Debezium delete envelope carries the deleted row in
        // "before" with "after" null, and removals must not be dropped (they would leave the mixed index stale).
        // Reindex-from-current-state only needs the changed element's coordinates (key + column1), which are present in
        // whichever image the event populates; the applier then reflects the element's current state (removing it if
        // it is now gone).
        JsonNode data = payload.get("after");
        if (data == null || data.isNull()) {
            data = payload.get("before");
        }
        if (data == null || data.isNull()) {
            return Collections.emptyList();
        }
        final byte[] keyBytes = decodeCell(data, "key");
        if (keyBytes == null) {
            return Collections.emptyList();
        }
        final Object vertexId;
        try {
            vertexId = idManager.getKeyID(new StaticArrayBuffer(keyBytes));
        } catch (RuntimeException e) {
            // The partition key bytes are malformed (a corrupt / poison-pill record): skip it rather than let the
            // exception propagate and make the worker rewind and reprocess the same undecodable record forever.
            // Consistent with the invalid-JSON and invalid-Base64 handling above.
            log.warn("Skipping CDC record: could not decode the partition key to a vertex id", e);
            return Collections.emptyList();
        }

        final byte[] columnBytes = decodeCell(data, "column1");
        if (columnBytes == null) {
            // Partition-level mutation (e.g. whole-vertex delete): reindex the vertex from current state.
            return Collections.singletonList(vertexChange(vertexId));
        }
        return resolveChanges(vertexId, columnBytes, decodeCell(data, "value"));
    }

    private Collection<CdcElementChange> resolveChanges(Object vertexId, byte[] columnBytes, byte[] valueBytes) {
        final StaticBuffer column = new StaticArrayBuffer(columnBytes);
        final StaticBuffer value = new StaticArrayBuffer(valueBytes != null ? valueBytes : new byte[0]);
        final Entry entry = StaticArrayEntry.of(column, value);
        // The transaction serves only as a schema TypeInspector for the header parse below; those lookups are served
        // from the graph's shared schema cache, so opening one per record costs an allocation, not backend reads.
        // (Reusing one transaction across a poll batch would require a batch-scoped decoder SPI for an unmeasured gain.)
        final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        try {
            // Reindex-from-current-state only needs to identify WHICH element changed, not whether the Debezium op was
            // an insert, update, or delete: the applier reads each element's current state (reindexing it, or removing
            // its document when it is now gone). So only the raw relation header is parsed here: the type, the relation
            // id, and (for edges) the direction and other endpoint. Parsing the header directly -- instead of fully
            // reconstructing the relation -- also handles the shapes a full reconstruction cannot:
            //  - IN-direction columns (each edge is stored twice, once per endpoint): the row owner is the IN vertex,
            //    so out/in are assigned by the parsed direction rather than assuming the row owner is the out-vertex.
            //  - Delete events, whose Cassandra tombstones carry no value bytes: for MULTI edges the relation id and
            //    other endpoint live in the column and remain recoverable, so the edge document can still be removed.
            // parseHeaderOnly=true is essential for that last case: a full parse would additionally read the label's
            // signature and inline properties from the (empty) value region and throw, losing the edge identity.
            final RelationCache cache = graph.getEdgeSerializer().readRelation(entry, true, tx);
            if (IDManager.isSystemRelationTypeId(cache.typeId)) {
                // System columns (e.g. the VertexExists marker) carry no index-relevant data of their own.
                return Collections.singletonList(vertexChange(vertexId));
            }
            final RelationType type = tx.getExistingRelationType(cache.typeId);
            if (type.isEdgeLabel() && cache.relationId > 0) {
                // Deliberately the RAW row/column ids, never canonicalVertexId(): for edges of partitioned vertices,
                // the user-facing edge id -- and with it the relation index document id, which embeds both endpoint
                // ids -- is built from the PARTITION-REPRESENTATIVE vertex the edge was assigned to, exactly the id
                // under which the edge's row copies are stored and which its columns reference. Canonicalizing here
                // would produce an edge identity (and document id) that matches nothing the synchronous path wrote.
                // (Vertex DOCUMENTS are the opposite case: they are keyed by the canonical id, hence vertexChange().)
                final Object otherVertexId = cache.getOtherVertexId();
                final Object outVertexId = cache.direction == Direction.OUT ? vertexId : otherVertexId;
                final Object inVertexId = cache.direction == Direction.OUT ? otherVertexId : vertexId;
                return Collections.singletonList(new CdcElementChange(ElementCategory.EDGE,
                    new RelationIdentifier(outVertexId, cache.typeId, cache.relationId, inVertexId)));
            }
            if (type.isPropertyKey()) {
                // A property column changed: reindex the owning vertex (for vertex indexes on the property key) and,
                // when the property's relation id is recoverable, the property element itself (for property-element
                // mixed indexes). As above, the RelationIdentifier keeps the raw row-owner id.
                final List<CdcElementChange> changes = new ArrayList<>(2);
                changes.add(vertexChange(vertexId));
                if (cache.relationId > 0) {
                    changes.add(new CdcElementChange(ElementCategory.PROPERTY,
                        new RelationIdentifier(vertexId, cache.typeId, cache.relationId, null)));
                }
                return changes;
            }
            // Edge column without a recoverable relation id: a constrained-multiplicity edge's id lives in the value
            // region, which a delete tombstone does not carry. Reindex both endpoints from current state -- their
            // vertex documents stay correct; see the documented cdc-only limitations for the edge document itself.
            final List<CdcElementChange> changes = new ArrayList<>(2);
            changes.add(vertexChange(vertexId));
            if (cache.getOtherVertexId() != null) {
                changes.add(vertexChange(cache.getOtherVertexId()));
            }
            return changes;
        } catch (RuntimeException | AssertionError e) {
            if (e instanceof RuntimeException && isBackendFailure(e)) {
                // Not a malformed record: the schema lookups inside the parse (type definitions load from storage on
                // a schema-cache miss) failed on the backend. Falling back would downgrade this event to a vertex-only
                // change and the batch would then commit -- permanently dropping e.g. an edge-document removal.
                // Propagate instead so the worker rewinds and the record is redelivered (at-least-once).
                throw (RuntimeException) e;
            }
            // Columns whose relation header cannot be parsed (e.g. a property delete tombstone, whose value region --
            // and with it the property value the parser expects -- is absent) fall back to reindexing the owning
            // vertex from current state, which keeps all vertex-element indexes correct. A dropped relation type also
            // lands here (its schema lookup fails without a backend cause), which converges the same way.
            log.debug("Could not parse the changed relation for vertex {}; reindexing the vertex instead", vertexId, e);
            return Collections.singletonList(vertexChange(vertexId));
        } finally {
            if (tx.isOpen()) {
                tx.rollback();
            }
        }
    }

    /** A VERTEX change for this (row-owner or column-referenced) vertex id, canonicalized: all six emission sites
     *  must route through here so none can miss the partitioned-vertex mapping below. */
    private CdcElementChange vertexChange(Object vertexId) {
        return new CdcElementChange(ElementCategory.VERTEX, canonicalVertexId(vertexId));
    }

    /**
     * Maps a partition-local id of a partitioned vertex to its canonical id. The adjacency of a partitioned vertex
     * (cluster.max-partitions &gt; 1 with a partitioned vertex label) is spread across per-partition rows whose keys
     * decode to partition-local ids, but the vertex's index document is keyed by the canonical id (reads canonicalize,
     * and index writes come from canonically-identified vertex objects) -- a document removal issued under a
     * partition-local id would silently miss it. Non-partitioned and custom (String) ids pass through unchanged.
     *
     * <p>Applied to VERTEX changes ONLY. The ids embedded in {@link RelationIdentifier}s (edge/property changes) must
     * stay raw: a partitioned vertex's relations are identified by the partition-representative id they were assigned
     * to -- that is what the user-facing relation id and therefore the relation document id embed (see the comment in
     * {@code resolveChanges}), and {@code DebeziumCassandraJsonDecoderTest#canonicalizesPartitionedVertexIdsFromPartitionCopyRows}
     * pins down both directions of this contract.</p>
     */
    private Object canonicalVertexId(Object vertexId) {
        if (vertexId instanceof Long && idManager.isPartitionedVertex(vertexId)) {
            return idManager.getCanonicalVertexId((Long) vertexId);
        }
        return vertexId;
    }

    /** Whether the failure is backend-caused (a {@link BackendException} anywhere in the cause chain), as opposed to
     *  a malformed record: backend failures are transient and must be redelivered, not skipped. */
    static boolean isBackendFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            if (cause instanceof BackendException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads a Cassandra blob cell from Debezium's {@code after} struct. Debezium wraps each column as
     * {@code {"value": <base64>, "deletion_ts": ..., "set": ...}}; some pipelines flatten it to a bare value.
     *
     * @return the decoded bytes, or {@code null} if the column is absent/null
     */
    private byte[] decodeCell(JsonNode after, String columnName) {
        final JsonNode cell = after.get(columnName);
        if (cell == null || cell.isNull()) {
            return null;
        }
        final JsonNode valueNode = cell.isObject() ? cell.get("value") : cell;
        if (valueNode == null || valueNode.isNull() || !valueNode.isTextual()) {
            return null;
        }
        try {
            return Base64.getDecoder().decode(valueNode.asText());
        } catch (IllegalArgumentException e) {
            // A corrupt / poison-pill record (non-Base64 blob). Skip it (return null) rather than let the exception
            // propagate and make the worker rewind-and-retry the same undecodable record forever (no forward progress).
            log.warn("Skipping CDC record: column '{}' is not valid Base64", columnName, e);
            return null;
        }
    }
}
