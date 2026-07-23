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

package org.janusgraph.graphdb.database.index;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applies mixed-index updates by <em>reindexing changed elements from their current graph state</em>, the same
 * technique JanusGraph's transaction recovery uses ({@code StandardTransactionLogProcessor.restoreExternalIndexes}).
 *
 * <p>Given a set of {@link CdcElementChange}s (which only identify <em>which</em> elements changed), it loads each
 * element's current state and fully replaces its index document via {@link org.janusgraph.diskstorage.indexing.IndexProvider#restore}
 * (a deleted element results in document removal). Because every application reads the current state, applying changes
 * out of order or more than once converges to the same result &mdash; the property that lets the CDC pipeline guarantee
 * eventual consistency without strict ordering.</p>
 *
 * <p>This class is reused by the {@code janusgraph-cdc} worker. It is backend-agnostic: the index backend (e.g.
 * ElasticSearch) is whatever the supplied {@link StandardJanusGraph} is configured with.</p>
 */
public final class MixedIndexUpdateApplier {

    private static final Logger log = LoggerFactory.getLogger(MixedIndexUpdateApplier.class);

    private final StandardJanusGraph graph;
    private final Duration maxWriteTime;

    /** backing index name &rarr; ids of the CDC-managed mixed indexes stored there. */
    private final Map<String, List<Long>> cdcIndexIdsByBacking;

    /**
     * @param graph          the graph whose schema and index backends are used
     * @param backingIndexes the backing index names that are maintained via CDC (e.g. {@code "search"})
     */
    public MixedIndexUpdateApplier(StandardJanusGraph graph, Set<String> backingIndexes) {
        this.graph = graph;
        this.maxWriteTime = graph.getConfiguration().getMaxWriteTime();
        this.cdcIndexIdsByBacking = enumerateCdcMixedIndexes(graph, backingIndexes);
    }

    /** The backing index names this applier maintains (those mixed indexes accepted by the filter). */
    public Set<String> getManagedBackingIndexes() {
        return cdcIndexIdsByBacking.keySet();
    }

    private static Map<String, List<Long>> enumerateCdcMixedIndexes(StandardJanusGraph graph,
                                                                    Set<String> backingIndexes) {
        final Map<String, List<Long>> result = new HashMap<>();
        final JanusGraphManagement mgmt = graph.openManagement();
        try {
            for (ElementCategory category : ElementCategory.values()) {
                for (JanusGraphIndex index : mgmt.getGraphIndexes(category.getElementType())) {
                    if (index.isMixedIndex() && backingIndexes.contains(index.getBackingIndex())) {
                        result.computeIfAbsent(index.getBackingIndex(), k -> new ArrayList<>()).add(index.longId());
                    }
                }
            }
        } finally {
            mgmt.rollback();
        }
        // Freeze the lists too: this instance is shared across CDC worker threads.
        result.replaceAll((k, v) -> Collections.unmodifiableList(v));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Reindexes every supplied changed element into the CDC-managed mixed indexes, issuing one index-backend
     * commit (e.g. one ElasticSearch {@code _bulk}) per backing index. Idempotent and order-independent.
     */
    public void apply(Collection<CdcElementChange> changes) {
        if (changes.isEmpty() || cdcIndexIdsByBacking.isEmpty()) {
            return;
        }
        // Group the changes by element category once so each mixed index only iterates its own category's changes,
        // rather than rescanning the whole change set (and re-checking the category) once per index.
        final Map<ElementCategory, List<CdcElementChange>> changesByCategory = new EnumMap<>(ElementCategory.class);
        for (CdcElementChange change : changes) {
            changesByCategory.computeIfAbsent(change.getCategory(), k -> new ArrayList<>()).add(change);
        }
        // One retried unit spanning ALL backing indexes: the batch shares one transaction (one element load feeds
        // every backing) and still issues one restore (e.g. one ES _bulk) per backing. The transaction is created
        // INSIDE the retried callable so each BackendOperation retry runs against a fresh tx reflecting the current
        // graph state; a retried attempt simply recomputes and re-restores -- restore() fully replaces documents, so
        // a partially-applied earlier attempt converges (idempotent).
        BackendOperation.execute(() -> {
            // skipDBCacheRead: reindex-from-current-state must read the LIVE graph. This applier typically runs in a
            // dedicated worker JVM whose database-level cache (cache.db-cache) is never invalidated by the remote
            // JVMs committing the changes -- a cached read here could reindex a stale row image (or resurrect a
            // deleted element) and, with offsets then committed, the index would permanently miss the change.
            final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().skipDBCacheRead().start();
            try {
                // Load each changed element at most once for the whole batch (across all backing indexes and all
                // mixed indexes of the same element category). Misses (deleted elements -> null) are cached
                // explicitly, since the delete case is exactly where repeated lookups would otherwise occur.
                final Map<Object, JanusGraphElement> elementCache =
                    preloadVertices(tx, changesByCategory.getOrDefault(ElementCategory.VERTEX, Collections.emptyList()));
                boolean wroteAny = false;
                for (Map.Entry<String, List<Long>> entry : cdcIndexIdsByBacking.entrySet()) {
                    final Map<String, Map<String, List<IndexEntry>>> documentsPerStore =
                        collectDocuments(tx, entry.getKey(), entry.getValue(), changesByCategory, elementCache);
                    if (!documentsPerStore.isEmpty()) {
                        // restore() applies immediately at the index provider; the tx commit below finalizes it.
                        tx.getTxHandle().getIndexTransaction(entry.getKey()).restore(documentsPerStore);
                        wroteAny = true;
                    }
                    // else: none of the changes matched this backing's element categories -- skip the otherwise
                    // no-op restore round-trip (e.g. an empty ES _bulk).
                }
                if (wroteAny) {
                    // Commit the enclosing graph transaction (which commits every index transaction and closes the
                    // tx) rather than the index transactions directly: committing an indexTx alone would leave the
                    // parent tx to be rolled back in the finally block, and that rollback also reaches the
                    // already-committed index transaction (BackendTransaction.rollback() rolls back every index tx)
                    // -- harmless for the in-tree providers, but contract-murky for other index SPI implementations.
                    // This way the finally rollback runs only on exception (or nothing-to-write) paths.
                    tx.commit();
                }
                return true;
            } finally {
                if (tx.isOpen()) {
                    tx.rollback();
                }
            }
        }, maxWriteTime);
    }

    /**
     * Batch-loads the changed vertices (one multiget for existence, one sliced multiget for their properties, both
     * populating the transaction's vertex-centric cache so the per-key reads in
     * {@link IndexSerializer#reindexElement} become cache hits) instead of one storage round-trip per vertex.
     * Requested ids that do not resolve (deleted vertices) are cached as {@code null} so the per-index loop routes
     * them straight to document removal without a redundant per-id existence read. EDGE/PROPERTY changes are
     * resolved per element by the caller ({@link RelationIdentifier} lookups have no batched equivalent).
     */
    private Map<Object, JanusGraphElement> preloadVertices(StandardJanusGraphTx tx, List<CdcElementChange> vertexChanges) {
        final Map<Object, JanusGraphElement> elementCache = new HashMap<>();
        if (vertexChanges.isEmpty()) {
            return elementCache;
        }
        final Set<Object> ids = new LinkedHashSet<>();
        for (CdcElementChange change : vertexChanges) {
            ids.add(change.getElementId());
        }
        try {
            final List<JanusGraphVertex> vertices = new ArrayList<>(ids.size());
            for (JanusGraphVertex v : tx.getVertices(ids.toArray())) {
                vertices.add(v);
            }
            if (!vertices.isEmpty()) {
                tx.multiQuery(vertices).properties();
                for (JanusGraphVertex v : vertices) {
                    elementCache.put(v.id(), v);
                }
            }
            for (Object id : ids) {
                elementCache.putIfAbsent(id, null); // requested but not returned -> deleted: cache the miss
            }
        } catch (RuntimeException e) {
            // E.g. an id whose type the graph's id regime rejects (getVertices throws where the per-element
            // tx.getVertex returns null). Degrade to the per-element loads in the caller -- correctness does not
            // depend on this preload, it is purely a batching optimization.
            log.debug("Batched vertex preload failed; falling back to per-element loads", e);
            elementCache.clear();
        }
        return elementCache;
    }

    /** Collects the full replacement documents (or removals) for one backing index's mixed indexes. */
    private Map<String, Map<String, List<IndexEntry>>> collectDocuments(StandardJanusGraphTx tx, String backingIndexName,
                                                                        List<Long> indexIds,
                                                                        Map<ElementCategory, List<CdcElementChange>> changesByCategory,
                                                                        Map<Object, JanusGraphElement> elementCache) {
        final Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
        for (Long indexId : indexIds) {
            final JanusGraphSchemaVertex schemaVertex = (JanusGraphSchemaVertex) tx.getVertex(indexId);
            if (schemaVertex == null) {
                // The mixed index was dropped (or its schema id changed) after this applier enumerated it at
                // startup. A dropped index needs no maintenance, so skip it (with a clear log) rather than
                // NPE on asIndexType() and wedge the worker in an endless failing-retry loop.
                log.warn("CDC-managed mixed index (id {}) on backing index '{}' no longer resolves "
                    + "(was it dropped?); skipping it", indexId, backingIndexName);
                continue;
            }
            final MixedIndexType index = (MixedIndexType) schemaVertex.asIndexType();
            if (allFieldsDisabled(index)) {
                // A fully DISABLED (decommissioned) index is no longer maintained by the synchronous write
                // path either -- it skips DISABLED fields and so writes nothing. It must not be actively
                // wiped here: reindexElement would return false for every element (all fields skipped) and
                // the removeElement fallback below would then delete the index's documents one by one.
                continue;
            }
            final ElementCategory category = index.getElement();
            for (CdcElementChange change : changesByCategory.getOrDefault(category, Collections.emptyList())) {
                final JanusGraphElement element;
                if (elementCache.containsKey(change.getElementId())) {
                    element = elementCache.get(change.getElementId());
                } else {
                    element = category.retrieve(change.getElementId(), tx);
                    elementCache.put(change.getElementId(), element);
                }
                if (element != null && graph.getIndexSerializer().reindexElement(element, index, documentsPerStore)) {
                    continue; // reindexed with the element's current indexed field values
                }
                if (element != null && !IndexRecordUtil.indexAppliesTo(index, element)) {
                    // The index does not apply to this live element (e.g. an indexOnly label restriction):
                    // the synchronous path never wrote a document for it, so there is nothing to remove --
                    // avoid emitting a spurious per-element delete into the index-backend batch.
                    continue;
                }
                // Element is gone, OR it still exists but no longer has any indexed field for this index (e.g.
                // all of its indexed properties were removed, so reindexElement wrote nothing and returned
                // false): remove any previously-written document so the index converges to the current state
                // instead of keeping a stale document.
                graph.getIndexSerializer().removeElement(change.getElementId(), index, documentsPerStore);
            }
        }
        return documentsPerStore;
    }

    private static boolean allFieldsDisabled(MixedIndexType index) {
        for (ParameterIndexField field : index.getFieldKeys()) {
            if (field.getStatus() != SchemaStatus.DISABLED) {
                return false;
            }
        }
        return true;
    }
}
