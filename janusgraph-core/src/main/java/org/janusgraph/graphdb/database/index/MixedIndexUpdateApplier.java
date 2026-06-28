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
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
import org.janusgraph.graphdb.internal.ElementCategory;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

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
     * @param graph              the graph whose schema and index backends are used
     * @param backingIndexFilter accepts the backing index names that are maintained via CDC (e.g. {@code "search"})
     */
    public MixedIndexUpdateApplier(StandardJanusGraph graph, Predicate<String> backingIndexFilter) {
        this.graph = graph;
        this.maxWriteTime = graph.getConfiguration().getMaxWriteTime();
        this.cdcIndexIdsByBacking = enumerateCdcMixedIndexes(graph, backingIndexFilter);
    }

    /** The backing index names this applier maintains (those mixed indexes accepted by the filter). */
    public Set<String> getManagedBackingIndexes() {
        return cdcIndexIdsByBacking.keySet();
    }

    private static Map<String, List<Long>> enumerateCdcMixedIndexes(StandardJanusGraph graph,
                                                                    Predicate<String> backingIndexFilter) {
        final Map<String, List<Long>> result = new HashMap<>();
        final JanusGraphManagement mgmt = graph.openManagement();
        try {
            for (ElementCategory category : ElementCategory.values()) {
                for (JanusGraphIndex index : mgmt.getGraphIndexes(category.getElementType())) {
                    if (index.isMixedIndex() && backingIndexFilter.test(index.getBackingIndex())) {
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
        for (Map.Entry<String, List<Long>> entry : cdcIndexIdsByBacking.entrySet()) {
            applyForBackingIndex(entry.getKey(), entry.getValue(), changesByCategory);
        }
    }

    private void applyForBackingIndex(String backingIndexName, List<Long> indexIds, Map<ElementCategory, List<CdcElementChange>> changesByCategory) {
        // The transaction (and its index transaction) are created INSIDE the retried callable so that each
        // BackendOperation retry runs against a fresh tx/indexTx reflecting the current graph state, rather than
        // reusing a transaction that a failed attempt may have left in an undefined state.
        BackendOperation.execute(() -> {
            final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
            try {
                final IndexTransaction indexTx = tx.getTxHandle().getIndexTransaction(backingIndexName);
                final Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
                // Load each changed element at most once per backing index: a backing may host several mixed indexes
                // of the same element category, which would otherwise re-read the same element from the graph. Misses
                // (deleted elements -> null) are cached explicitly, since computeIfAbsent would not store them and the
                // delete case is exactly where repeated lookups would otherwise occur.
                final Map<Object, JanusGraphElement> elementCache = new HashMap<>();
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
                if (documentsPerStore.isEmpty()) {
                    // None of the changes matched this backing index's element categories: there is nothing to write,
                    // so skip the otherwise no-op index-backend restore/commit round-trip (e.g. an empty ES _bulk).
                    // The unused index transaction is released by the rollback in the finally block.
                    return true;
                }
                indexTx.restore(documentsPerStore);
                // Commit the enclosing graph transaction (which commits the index transaction and closes the tx)
                // rather than the index transaction directly: committing indexTx alone would leave the parent tx to
                // be rolled back in the finally block, and that rollback also reaches the already-committed index
                // transaction (BackendTransaction.rollback() rolls back every index tx) -- harmless for the in-tree
                // providers, but contract-murky for other index SPI implementations. This way the finally rollback
                // runs only on exception paths.
                tx.commit();
                return true;
            } finally {
                if (tx.isOpen()) {
                    tx.rollback();
                }
            }
        }, maxWriteTime);
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
