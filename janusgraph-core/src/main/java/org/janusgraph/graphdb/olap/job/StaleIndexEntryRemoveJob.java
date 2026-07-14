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
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.olap.GraphProvider;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.relations.RelationIdentifierUtils;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.janusgraph.graphdb.database.util.IndexRecordUtil.bytebuffer2RelationId;

/**
 * Removes stale entries from a composite graph index: entries which reference graph elements that no longer
 * exist, for example because the element was deleted while an index status change was still propagating
 * through the cluster or while the index was disabled. This job complements {@link IndexRepairJob}: a repair
 * (reindex) restores missing entries for existing elements but never removes entries, while this job removes
 * entries of deleted elements but never adds entries.
 */
public class StaleIndexEntryRemoveJob extends IndexUpdateJob implements ScanJob {

    /**
     * Number of stale index entries (composite index columns or mixed index documents) that were removed.
     */
    public static final String REMOVED_RECORDS_COUNT = "stale-entries-removed";

    /**
     * Number of index entries that were examined.
     */
    public static final String SCANNED_RECORDS_COUNT = "scanned-entries";

    /**
     * Length of the column slice end bound which spans the entire column space of a composite index row.
     * This is the same bound {@link IndexRemoveJob} uses to remove all entries of an index: index entry
     * columns start with a zero marker byte (followed by the serialized element id for non-unique indexes),
     * so no column can sort beyond a run of 128 0xFF bytes.
     */
    private static final int INDEX_ENTRY_SLICE_END_LENGTH = 128;

    /**
     * Number of element existence checks after which the check transaction is recycled. Elements which exist
     * are cached by their transaction, so a bounded transaction lifetime keeps the memory footprint of a scan
     * worker bounded regardless of the index size.
     */
    private static final int EXISTENCE_CHECKS_PER_TX = 10_000;

    private final GraphProvider graph = new GraphProvider();

    private IndexSerializer indexSerializer;
    private long graphIndexId;
    private ElementCategory elementCategory;
    private StandardJanusGraphTx checkTx;
    private int checksInCheckTx;

    public StaleIndexEntryRemoveJob() {
        super();
    }

    protected StaleIndexEntryRemoveJob(StaleIndexEntryRemoveJob copy) {
        super(copy);
        if (copy.graph.isProvided()) this.graph.setGraph(copy.graph.get());
    }

    public StaleIndexEntryRemoveJob(final JanusGraph graph, final String indexName, final String indexType) {
        super(indexName, indexType);
        this.graph.setGraph(graph);
    }

    @Override
    public void workerIterationStart(Configuration config, Configuration graphConf, ScanMetrics metrics) {
        graph.initializeGraph(graphConf);
        indexSerializer = graph.get().getIndexSerializer();
        try {
            super.workerIterationStart(graph.get(), config, metrics);
        } catch (Throwable e) {
            graph.close();
            throw e;
        }
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        closeCheckTx();
        super.workerIterationEnd(metrics);
        graph.close();
    }

    @Override
    protected void validateIndexStatus() {
        Preconditions.checkArgument(index instanceof JanusGraphIndex && ((JanusGraphIndex) index).isCompositeIndex(),
            "The index [%s] is not a composite graph index. Stale entries can only be removed from global graph indexes.", indexName);
        CompositeIndexType indexType = (CompositeIndexType) managementSystem.getSchemaVertex(index).asIndexType();
        graphIndexId = indexType.longId();
        elementCategory = indexType.getElement();

        JanusGraphSchemaVertex schemaVertex = managementSystem.getSchemaVertex(index);
        SchemaStatus actualStatus = schemaVertex.getStatus();
        Set<SchemaStatus> acceptableStatuses = SchemaAction.REMOVE_STALE_ENTRIES.getApplicableStatus();
        Preconditions.checkArgument(acceptableStatuses.contains(actualStatus),
            "Stale entries cannot be removed from index [%s] with status [%s]. The status must be one of %s.",
            indexName, actualStatus, acceptableStatuses);
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        try {
            final List<Entry> staleEntries = new ArrayList<>();
            int scanned = 0;
            for (EntryList entryList : entries.values()) {
                for (Entry entry : entryList) {
                    scanned++;
                    Object elementId = readElementId(entry);
                    if (!elementExists(elementId)) {
                        staleEntries.add(entry);
                    }
                }
            }
            metrics.incrementCustom(SCANNED_RECORDS_COUNT, scanned);
            if (!staleEntries.isEmpty()) {
                BackendTransaction mutator = writeTx.getTxHandle();
                mutator.mutateIndex(key, KCVSCache.NO_ADDITIONS, staleEntries);
                metrics.incrementCustom(REMOVED_RECORDS_COUNT, staleEntries.size());
            }
        } catch (final Exception e) {
            closeCheckTx();
            managementSystem.rollback();
            writeTx.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new JanusGraphException(e.getMessage(), e);
        }
    }

    private Object readElementId(Entry entry) {
        final ReadBuffer readBuffer = entry.asReadBuffer();
        readBuffer.movePositionTo(entry.getValuePosition());
        if (elementCategory == ElementCategory.VERTEX) {
            return IDHandler.readVertexId(readBuffer, true);
        } else {
            return bytebuffer2RelationId(readBuffer);
        }
    }

    private boolean elementExists(Object elementId) {
        //Existence checks run in a dedicated short-lived read-only transaction which is recycled periodically:
        //elements which exist are cached by their transaction, so checking through the long-lived writeTx
        //would grow its cache without bound on large indexes
        if (checkTx == null || checksInCheckTx >= EXISTENCE_CHECKS_PER_TX) {
            closeCheckTx();
            checkTx = (StandardJanusGraphTx) graph.get().buildTransaction().readOnly().start();
        }
        checksInCheckTx++;
        if (elementCategory == ElementCategory.VERTEX) {
            return checkTx.getVertex(elementId) != null;
        }
        RelationIdentifier relationId = (RelationIdentifier) elementId;
        if (elementCategory == ElementCategory.EDGE) {
            return RelationIdentifierUtils.findEdge(relationId, checkTx) != null;
        }
        return RelationIdentifierUtils.findProperty(relationId, checkTx) != null;
    }

    private void closeCheckTx() {
        if (checkTx != null && checkTx.isOpen()) {
            //The existence checks are read-only
            checkTx.rollback();
        }
        checkTx = null;
        checksInCheckTx = 0;
    }

    @Override
    public List<SliceQuery> getQueries() {
        return Collections.singletonList(new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(INDEX_ENTRY_SLICE_END_LENGTH)));
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        Preconditions.checkArgument(graphIndexId > 0);
        return (k -> {
            try {
                return indexSerializer.getIndexIdFromKey(k) == graphIndexId;
            } catch (RuntimeException e) {
                log.error("Filtering key {} due to exception", k, e);
                return false;
            }
        });
    }

    @Override
    public StaleIndexEntryRemoveJob clone() {
        return new StaleIndexEntryRemoveJob(this);
    }
}
