// Copyright 2017 JanusGraph Authors
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
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.AbstractScanJob;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.graphdb.vertices.CacheVertex;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class GhostVertexRemover extends AbstractScanJob {

    private static final int RELATION_COUNT_LIMIT = 10000;

    private static final SliceQuery EVERYTHING_QUERY = new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(4));

    public static final String REMOVED_RELATION_COUNT = "removed-relations";
    public static final String REMOVED_VERTEX_COUNT = "removed-vertices";
    public static final String SKIPPED_GHOST_LIMIT_COUNT = "skipped-ghosts";

    private final SliceQuery everythingQueryLimit = EVERYTHING_QUERY.updateLimit(RELATION_COUNT_LIMIT);
    private Instant jobStartTime;

    public GhostVertexRemover(JanusGraph graph) {
        super(graph);
    }

    public GhostVertexRemover() {
        this((JanusGraph) null);
    }

    protected GhostVertexRemover(GhostVertexRemover copy) {
        super(copy);
    }

    @Override
    public GhostVertexRemover clone() {
        return new GhostVertexRemover(this);
    }

    @Override
    public void workerIterationStart(Configuration jobConfig, Configuration graphConfig, ScanMetrics metrics) {
        Preconditions.checkArgument(jobConfig.has(GraphDatabaseConfiguration.JOB_START_TIME), "Invalid configuration for this job. Start time is required.");
        jobStartTime = Instant.ofEpochMilli(jobConfig.get(GraphDatabaseConfiguration.JOB_START_TIME));
        open(graphConfig);
    }

    @Override
    protected StandardJanusGraphTx startTransaction(StandardJanusGraph graph) {
        assert jobStartTime != null;
        StandardTransactionBuilder txb = graph.buildTransaction();
        txb.commitTime(jobStartTime);
        txb.checkExternalVertexExistence(false);
        txb.checkInternalVertexExistence(false);
        return (StandardJanusGraphTx) txb.start();
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        long vertexId = getVertexId(key);
        assert entries.size() == 1;
        assert entries.get(everythingQueryLimit) != null;
        final EntryList everything = entries.get(everythingQueryLimit);
        if (!isGhostVertex(vertexId, everything)) {
            return;
        }
        if (everything.size() >= RELATION_COUNT_LIMIT) {
            metrics.incrementCustom(SKIPPED_GHOST_LIMIT_COUNT);
            return;
        }

        JanusGraphVertex vertex = tx.getInternalVertex(vertexId);
        Preconditions.checkArgument(vertex instanceof CacheVertex,
            "The bounding transaction is not configured correctly");
        CacheVertex v = (CacheVertex) vertex;
        v.loadRelations(EVERYTHING_QUERY, input -> everything);

        int removedRelations = 0;
        Iterator<JanusGraphRelation> iterator = v.query().noPartitionRestriction().relations().iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
            removedRelations++;
        }
        v.remove();
        //There should be no more system relations to remove
        metrics.incrementCustom(REMOVED_VERTEX_COUNT);
        metrics.incrementCustom(REMOVED_RELATION_COUNT, removedRelations);
    }

    @Override
    public void workerIterationEnd(final ScanMetrics metrics) {
        tx.commit();
        close();
    }

    @Override
    public List<SliceQuery> getQueries() {
        return Collections.singletonList(everythingQueryLimit);
    }
}
