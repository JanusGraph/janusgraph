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

package org.janusgraph.graphdb.olap.job;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.keycolumnvalue.scan.CompletableScanJobFuture;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJobFuture;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanMetrics;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.relations.RelationIdentifierUtils;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.janusgraph.graphdb.database.util.IndexRecordUtil.string2ElementId;

/**
 * Removes stale documents from a mixed graph index: documents which reference graph elements that no longer
 * exist, for example because the element was deleted while the index status change was still propagating
 * through the cluster or while the index was disabled.
 * <p>
 * Unlike {@link StaleIndexEntryRemoveJob} for composite indexes, the documents of a mixed index live in the
 * external index backend and cannot be scanned through the storage scan framework. Instead, the index is
 * enumerated by running one exists-query per indexed field key against the index backend. Fields which do not
 * support exists queries are skipped with a warning; if no field of the index supports exists queries, the
 * operation fails.
 */
public class MixedIndexStaleEntryRemover {

    private static final Logger log = LoggerFactory.getLogger(MixedIndexStaleEntryRemover.class);

    private MixedIndexStaleEntryRemover() {
    }

    /**
     * Starts the stale document removal for the given mixed index on a dedicated background thread.
     * Cancelling the returned future stops the removal at the next document boundary on a best-effort basis
     * (with {@code mayInterruptIfRunning} the worker thread is additionally interrupted); stale documents that
     * were already deleted stay deleted.
     * <p>
     * The removal runs on a daemon thread and is therefore best-effort: a JVM shutdown can cut it short
     * instead of delaying the shutdown until the cleanup has finished. This is safe because the job only ever
     * deletes documents of elements which no longer exist — a partially cleaned index remains consistent — and
     * the action can simply be re-run to remove the remaining stale documents.
     *
     * @param graph     the graph against which element existence is verified
     * @param indexName the name of the mixed graph index
     * @param batchSize number of stale documents that are deleted per batch
     * @return a future which completes with the job's {@link ScanMetrics} once the removal has finished
     */
    public static ScanJobFuture submit(StandardJanusGraph graph, String indexName, int batchSize) {
        Preconditions.checkArgument(batchSize > 0, "The batch size must be positive: %s", batchSize);
        StandardScanMetrics metrics = new StandardScanMetrics();
        CompletableFuture<ScanMetrics> future = new CompletableFuture<>();
        Thread worker = new Thread(() -> {
            try {
                run(graph, indexName, batchSize, metrics, future::isCancelled);
                future.complete(metrics);
            } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                    log.info("Stale entry removal for index [{}] was cancelled", indexName);
                } else {
                    log.error("Stale entry removal failed for index [{}]", indexName, t);
                }
                future.completeExceptionally(t);
            }
        }, "janusgraph-stale-index-cleanup-" + indexName);
        worker.setDaemon(true);
        worker.start();
        return new CompletableScanJobFuture(future, metrics, worker::interrupt);
    }

    private static void run(StandardJanusGraph graph, String indexName, int batchSize, ScanMetrics metrics,
                            BooleanSupplier cancelled) throws Exception {
        ManagementSystem managementSystem = (ManagementSystem) graph.openManagement();
        StandardJanusGraphTx tx = null;
        StandardJanusGraphTx checkTx = null;
        try {
            JanusGraphIndex index = managementSystem.getGraphIndex(indexName);
            Preconditions.checkArgument(index != null, "Could not find index: %s", indexName);
            Preconditions.checkArgument(index.isMixedIndex(), "The index [%s] is not a mixed index", indexName);
            MixedIndexType indexType = (MixedIndexType) managementSystem.getSchemaVertex(index).asIndexType();
            IndexSerializer indexSerializer = graph.getIndexSerializer();

            List<ParameterIndexField> queryableFields = selectQueryableFields(indexType, indexSerializer, indexName);
            ElementCategory elementCategory = indexType.getElement();
            String backingIndexName = indexType.getBackingIndexName();

            //This transaction only serves the index backend enumeration queries and the batched restore
            //calls; element existence is verified through short-lived transactions (see below) so that no
            //vertex cache can accumulate across the whole run
            tx = (StandardJanusGraphTx) graph.buildTransaction().start();
            BackendTransaction backendTx = tx.getTxHandle();

            //Ids scheduled for removal: guarantees that a document indexed under several fields is only
            //counted and deleted once. The set only ever holds ids of stale documents and is only needed
            //when more than one field is enumerated (within a single exists-query stream every document
            //appears at most once).
            Set<Object> removedIds = queryableFields.size() > 1 ? new HashSet<>() : null;
            Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
            int pendingDeletions = 0;
            int checksInCheckTx = 0;

            for (ParameterIndexField field : queryableFields) {
                IndexQuery existsQuery = indexSerializer.getQuery(indexType,
                    new PredicateCondition<>(field.getFieldKey(), Cmp.NOT_EQUAL, null), OrderList.NO_ORDER);
                try (Stream<String> documentIds = backendTx.indexQuery(backingIndexName, existsQuery)) {
                    Iterator<String> iterator = documentIds.iterator();
                    while (iterator.hasNext()) {
                        //Cooperative cancellation: cancel(false) does not interrupt the worker thread, so the
                        //cancellation state of the future is checked as well
                        if (Thread.interrupted() || cancelled.getAsBoolean()) {
                            throw new InterruptedException("Stale entry removal for index [" + indexName + "] was cancelled");
                        }
                        String documentId = iterator.next();
                        metrics.incrementCustom(StaleIndexEntryRemoveJob.SCANNED_RECORDS_COUNT);
                        Object elementId = string2ElementId(documentId);
                        if (removedIds != null && removedIds.contains(elementId)) {
                            continue;
                        }
                        //Existing elements are cached by their transaction, so the existence-check transaction
                        //is recycled after batchSize checks to keep the memory footprint of a run bounded
                        //regardless of the index size
                        if (checkTx == null || checksInCheckTx >= batchSize) {
                            if (checkTx != null) checkTx.rollback();
                            checkTx = (StandardJanusGraphTx) graph.buildTransaction().readOnly().start();
                            checksInCheckTx = 0;
                        }
                        checksInCheckTx++;
                        if (elementExists(elementId, elementCategory, checkTx)) {
                            continue;
                        }
                        if (removedIds != null) {
                            removedIds.add(elementId);
                        }
                        indexSerializer.removeElement(elementId, indexType, documentsPerStore);
                        metrics.incrementCustom(StaleIndexEntryRemoveJob.REMOVED_RECORDS_COUNT);
                        if (++pendingDeletions >= batchSize) {
                            backendTx.getIndexTransaction(backingIndexName).restore(documentsPerStore);
                            documentsPerStore = new HashMap<>();
                            pendingDeletions = 0;
                        }
                    }
                }
            }
            if (pendingDeletions > 0) {
                backendTx.getIndexTransaction(backingIndexName).restore(documentsPerStore);
            }

            tx.commit();
            //The management transaction is only used to read the index definition: close it without
            //committing so that no schema mutation is signaled
            managementSystem.rollback();
            log.info("Removed {} stale document(s) from index [{}]",
                metrics.getCustom(StaleIndexEntryRemoveJob.REMOVED_RECORDS_COUNT), indexName);
        } catch (Throwable t) {
            if (tx != null && tx.isOpen()) tx.rollback();
            if (managementSystem.isOpen()) managementSystem.rollback();
            throw t;
        } finally {
            //The existence checks are read-only
            if (checkTx != null && checkTx.isOpen()) checkTx.rollback();
        }
    }

    private static List<ParameterIndexField> selectQueryableFields(MixedIndexType indexType,
                                                                   IndexSerializer indexSerializer,
                                                                   String indexName) {
        Set<SchemaStatus> acceptableStatuses = SchemaAction.REMOVE_STALE_ENTRIES.getApplicableStatus();
        List<ParameterIndexField> queryableFields = new ArrayList<>();
        boolean anyApplicableField = false;
        for (ParameterIndexField field : indexType.getFieldKeys()) {
            SchemaStatus status = field.getStatus();
            if (!acceptableStatuses.contains(status)) continue;
            anyApplicableField = true;
            if (indexSerializer.supportsExistsQuery(indexType, field)) {
                queryableFields.add(field);
            } else {
                log.warn("Field [{}] of index [{}] does not support exists queries and cannot be scanned" +
                    " for stale entries", field.getFieldKey().name(), indexName);
            }
        }
        Preconditions.checkArgument(anyApplicableField,
            "Stale entries cannot be removed from index [%s]: no index field has one of the applicable statuses %s.",
            indexName, acceptableStatuses);
        Preconditions.checkArgument(!queryableFields.isEmpty(),
            "Stale entries cannot be removed from index [%s]: none of its fields supports exists queries.", indexName);
        return queryableFields;
    }

    private static boolean elementExists(Object elementId, ElementCategory elementCategory, StandardJanusGraphTx tx) {
        //Existing vertices are cached by the transaction, so repeated checks of the same live element are cheap
        if (elementCategory == ElementCategory.VERTEX) {
            return tx.getVertex(elementId) != null;
        }
        RelationIdentifier relationId = (RelationIdentifier) elementId;
        if (elementCategory == ElementCategory.EDGE) {
            return RelationIdentifierUtils.findEdge(relationId, tx) != null;
        }
        return RelationIdentifierUtils.findProperty(relationId, tx) != null;
    }
}
