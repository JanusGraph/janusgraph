// Copyright 2025 JanusGraph Authors
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

package org.janusgraph.diskstorage.cdc;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Wrapper around IndexTransaction that captures mutations and publishes them as CDC events.
 * Supports different CDC modes: skip, dual, or cdc-only.
 */
public class CdcIndexTransaction {

    private static final Logger log = LoggerFactory.getLogger(CdcIndexTransaction.class);

    public enum CdcMode {
        SKIP,      // Skip mutations during transaction, rely entirely on CDC
        DUAL,      // Write during transaction AND via CDC for consistency
        CDC_ONLY   // Only via CDC (deprecated, same as SKIP for now)
    }

    private final IndexTransaction delegate;
    private final CdcProducer cdcProducer;
    private final CdcMode cdcMode;
    private final Map<String, Map<String, MutationAccumulator>> pendingMutations;

    private static class MutationAccumulator {
        final List<IndexEntry> additions = new ArrayList<>();
        final List<IndexEntry> deletions = new ArrayList<>();
        boolean isNew;
        boolean isDeleted;
        final long timestamp;

        MutationAccumulator(boolean isNew, boolean isDeleted) {
            this.isNew = isNew;
            this.isDeleted = isDeleted;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public CdcIndexTransaction(IndexTransaction delegate, CdcProducer cdcProducer, CdcMode cdcMode) {
        this.delegate = delegate;
        this.cdcProducer = cdcProducer;
        this.cdcMode = cdcMode;
        this.pendingMutations = new HashMap<>();
        log.debug("Created CdcIndexTransaction with mode: {}", cdcMode);
    }

    public void add(String store, String documentId, IndexEntry entry, boolean isNew) {
        // Accumulate mutations for CDC
        accumulateAddition(store, documentId, entry, isNew);

        // Forward to delegate based on mode
        if (cdcMode != CdcMode.SKIP && cdcMode != CdcMode.CDC_ONLY) {
            delegate.add(store, documentId, entry, isNew);
        }
    }

    public void add(String store, String documentId, String key, Object value, boolean isNew) {
        // Accumulate mutations for CDC
        accumulateAddition(store, documentId, new IndexEntry(key, value), isNew);

        // Forward to delegate based on mode
        if (cdcMode != CdcMode.SKIP && cdcMode != CdcMode.CDC_ONLY) {
            delegate.add(store, documentId, key, value, isNew);
        }
    }

    public void delete(String store, String documentId, String key, Object value, boolean deleteAll) {
        // Accumulate mutations for CDC
        accumulateDeletion(store, documentId, new IndexEntry(key, value), deleteAll);

        // Forward to delegate based on mode
        if (cdcMode != CdcMode.SKIP && cdcMode != CdcMode.CDC_ONLY) {
            delegate.delete(store, documentId, key, value, deleteAll);
        }
    }

    private void accumulateAddition(String store, String documentId, IndexEntry entry, boolean isNew) {
        MutationAccumulator accumulator = getOrCreateAccumulator(store, documentId, isNew, false);
        accumulator.additions.add(entry);
    }

    private void accumulateDeletion(String store, String documentId, IndexEntry entry, boolean deleteAll) {
        MutationAccumulator accumulator = getOrCreateAccumulator(store, documentId, false, deleteAll);
        accumulator.deletions.add(entry);
    }

    private MutationAccumulator getOrCreateAccumulator(String store, String documentId, 
                                                       boolean isNew, boolean isDeleted) {
        Map<String, MutationAccumulator> storeAccumulator = 
            pendingMutations.computeIfAbsent(store, k -> new HashMap<>());
        
        MutationAccumulator accumulator = storeAccumulator.get(documentId);
        if (accumulator == null) {
            accumulator = new MutationAccumulator(isNew, isDeleted);
            storeAccumulator.put(documentId, accumulator);
        } else {
            // Update flags if needed
            if (isNew) accumulator.isNew = true;
            if (isDeleted) accumulator.isDeleted = true;
        }
        return accumulator;
    }

    public void commit() throws BackendException {
        // Publish CDC events first
        publishCdcEvents();

        // Then commit the delegate transaction
        delegate.commit();
    }

    private void publishCdcEvents() throws BackendException {
        if (pendingMutations.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Map<String, MutationAccumulator>> storeEntry : pendingMutations.entrySet()) {
            String storeName = storeEntry.getKey();
            for (Map.Entry<String, MutationAccumulator> docEntry : storeEntry.getValue().entrySet()) {
                String documentId = docEntry.getKey();
                MutationAccumulator accumulator = docEntry.getValue();

                CdcMutationEvent event = new CdcMutationEvent(
                    storeName,
                    documentId,
                    accumulator.additions,
                    accumulator.deletions,
                    accumulator.isNew,
                    accumulator.isDeleted,
                    accumulator.timestamp
                );

                cdcProducer.send(event);
            }
        }

        cdcProducer.flush();
        pendingMutations.clear();
    }

    public void rollback() throws BackendException {
        pendingMutations.clear();
        delegate.rollback();
    }

    // Delegate all other methods to the underlying transaction

    public void clearStorage() throws BackendException {
        delegate.clearStorage();
    }

    public void clearStore(String storeName) throws BackendException {
        delegate.clearStore(storeName);
    }

    public void register(String store, String key, KeyInformation information) throws BackendException {
        delegate.register(store, key, information);
    }

    public Stream<String> queryStream(IndexQuery query) throws BackendException {
        return delegate.queryStream(query);
    }

    public Number queryAggregation(IndexQuery query, Aggregation aggregation) throws BackendException {
        return delegate.queryAggregation(query, aggregation);
    }

    public Stream<RawQuery.Result<String>> queryStream(RawQuery query) throws BackendException {
        return delegate.queryStream(query);
    }

    public Long totals(RawQuery query) throws BackendException {
        return delegate.totals(query);
    }

    public void restore(Map<String, Map<String, List<IndexEntry>>> documents) throws BackendException {
        delegate.restore(documents);
    }

    public void logMutations(DataOutput out) {
        delegate.logMutations(out);
    }

    public void invalidate(String store) {
        delegate.invalidate(store);
    }
}
