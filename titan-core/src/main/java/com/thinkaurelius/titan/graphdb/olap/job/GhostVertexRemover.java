package com.thinkaurelius.titan.graphdb.olap.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class GhostVertexRemover extends VertexJobConverter {

    private static final int RELATION_COUNT_LIMIT = 10000;

    private static final SliceQuery EVERYTHING_QUERY = new SliceQuery(BufferUtil.zeroBuffer(1),BufferUtil.oneBuffer(4));

    public static final String REMOVED_RELATION_COUNT = "removed-relations";
    public static final String REMOVED_VERTEX_COUNT = "removed-vertices";
    public static final String SKIPPED_GHOST_LIMIT_COUNT = "skipped-ghosts";

    private final SliceQuery everythingQueryLimit = EVERYTHING_QUERY.updateLimit(RELATION_COUNT_LIMIT);
    private Instant jobStartTime;

    public GhostVertexRemover(TitanGraph graph) {
        super(graph, new NoOpJob());
    }

    public GhostVertexRemover() {
        this((TitanGraph)null);
    }

    protected GhostVertexRemover(GhostVertexRemover copy) { super(copy); }

    @Override
    public GhostVertexRemover clone() { return new GhostVertexRemover(this); }


    @Override
    public void workerIterationStart(Configuration jobConfig, Configuration graphConfig, ScanMetrics metrics) {
        super.workerIterationStart(jobConfig, graphConfig, metrics);
        Preconditions.checkArgument(jobConfig.has(GraphDatabaseConfiguration.JOB_START_TIME),"Invalid configuration for this job. Start time is required.");
        this.jobStartTime = Instant.ofEpochMilli(jobConfig.get(GraphDatabaseConfiguration.JOB_START_TIME));

        assert tx!=null && tx.isOpen();
        tx.rollback();
        StandardTransactionBuilder txb = graph.get().buildTransaction();
        txb.commitTime(jobStartTime);
        txb.checkExternalVertexExistence(false);
        txb.checkInternalVertexExistence(false);
        tx = (StandardTitanTx)txb.start();
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        long vertexId = getVertexId(key);
        assert entries.size()==1;
        assert entries.get(everythingQueryLimit)!=null;
        final EntryList everything = entries.get(everythingQueryLimit);
        if (!isGhostVertex(vertexId, everything)) {
            return;
        }
        if (everything.size()>=RELATION_COUNT_LIMIT) {
            metrics.incrementCustom(SKIPPED_GHOST_LIMIT_COUNT);
            return;
        }

        TitanVertex vertex = tx.getInternalVertex(vertexId);
        Preconditions.checkArgument(vertex instanceof CacheVertex,
                "The bounding transaction is not configured correctly");
        CacheVertex v = (CacheVertex)vertex;
        v.loadRelations(EVERYTHING_QUERY,new Retriever<SliceQuery, EntryList>() {
            @Override
            public EntryList get(SliceQuery input) {
                return everything;
            }
        });

        int removedRelations = 0;
        Iterator<TitanRelation> iter = v.query().noPartitionRestriction().relations().iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
            removedRelations++;
        }
        //There should be no more system relations to remove
        metrics.incrementCustom(REMOVED_VERTEX_COUNT);
        metrics.incrementCustom(REMOVED_RELATION_COUNT,removedRelations);
    }

    @Override
    public List<SliceQuery> getQueries() {
        return ImmutableList.of(everythingQueryLimit);
    }

    private static class NoOpJob implements VertexScanJob {

        @Override
        public void process(TitanVertex vertex, ScanMetrics metrics) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getQueries(QueryContainer queries) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NoOpJob clone() {
            return this;
        }
    }

}
