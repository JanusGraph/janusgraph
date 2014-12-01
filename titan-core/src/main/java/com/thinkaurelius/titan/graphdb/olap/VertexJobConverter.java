package com.thinkaurelius.titan.graphdb.olap;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexJobConverter implements ScanJob {

    private static final Logger log =
            LoggerFactory.getLogger(VertexJobConverter.class);

    protected static final SliceQuery VERTEX_EXISTS_QUERY = new SliceQuery(BufferUtil.zeroBuffer(4),BufferUtil.oneBuffer(4)).setLimit(1);

    public static final String GHOST_VERTEX_COUNT = "ghost-vertices";
    /**
     * Number of result sets that got (possibly) truncated due to an applied query limit
     */
    public static final String TRUNCATED_ENTRY_LISTS = "truncated-results";

    protected final GraphProvider graph;
    protected final VertexScanJob job;

    protected StandardTitanTx tx;
    private IDManager idManager;

    protected VertexJobConverter(TitanGraph graph, VertexScanJob job) {
        Preconditions.checkArgument(job!=null);
        this.graph = new GraphProvider();
        this.graph.setGraph(graph);
        this.job = job;
    }

    public static ScanJob convert(TitanGraph graph, VertexScanJob vertexJob) {
        return new VertexJobConverter(graph,vertexJob);
    }

    public static ScanJob convert(VertexScanJob vertexJob) {
        return new VertexJobConverter(null,vertexJob);
    }

    @Override
    public void setup(Configuration config, ScanMetrics metrics) {
        graph.initializeGraph(config);
        idManager = graph.get().getIDManager();
        StandardTransactionBuilder txb = graph.get().buildTransaction().readOnly();
        txb.setPreloadedData(true);
        txb.checkInternalVertexExistence(false);
        txb.dirtyVertexSize(0);
        txb.vertexCacheSize(500);
        try {
            tx = (StandardTitanTx)txb.start();
            job.setup(graph.get(), config, metrics);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    private void close() {
        if (null != tx && tx.isOpen())
            tx.rollback();
        graph.close();
    }

    @Override
    public void teardown(ScanMetrics metrics) {
        job.teardown(metrics);
        close();
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        long vertexId = getVertexId(key);
        assert entries.get(VERTEX_EXISTS_QUERY)!=null;
        if (isGhostVertex(vertexId, entries.get(VERTEX_EXISTS_QUERY))) {
            metrics.incrementCustom(GHOST_VERTEX_COUNT);
            return;
        }
        TitanVertex vertex = tx.getInternalVertex(vertexId);
        Preconditions.checkArgument(vertex instanceof PreloadedVertex,
                "The bounding transaction is not configured correctly");
        PreloadedVertex v = (PreloadedVertex)vertex;
        for (Map.Entry<SliceQuery,EntryList> entry : entries.entrySet()) {
            SliceQuery sq = entry.getKey();
            if (sq.equals(VERTEX_EXISTS_QUERY)) continue;
            EntryList entryList = entry.getValue();
            if (entryList.size()>=sq.getLimit()) metrics.incrementCustom(TRUNCATED_ENTRY_LISTS);
            v.addToQueryCache(sq.updateLimit(Query.NO_LIMIT),entryList);
        }
        try {
            job.process(v, metrics);
        } catch (Throwable ex) {
            log.error("Exception processing vertex [" + vertexId + "]: ", ex);
        }
    }

    protected boolean isGhostVertex(long vertexId, EntryList firstEntries) {
        if (idManager.isPartitionedVertex(vertexId) && !idManager.isCanonicalVertexId(vertexId)) return false;

        RelationCache relCache = tx.getEdgeSerializer().parseRelation(
                firstEntries.get(0),true,tx);
        return relCache.typeId != BaseKey.VertexExists.longId();
    }

    @Override
    public List<SliceQuery> getQueries() {
        try {
            QueryContainer qc = new QueryContainer(tx);
            job.getQueries(qc);

            List<SliceQuery> slices = new ArrayList<>();
            slices.add(VERTEX_EXISTS_QUERY);
            slices.addAll(qc.getSliceQueries());
            return slices;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        return buffer -> {
            long vertexId = getVertexId(buffer);
            if (IDManager.VertexIDType.Invisible.is(vertexId)) return false;
            else return true;
        };
    }

    protected long getVertexId(StaticBuffer key) {
        return idManager.getKeyID(key);
    }

    public static class GraphProvider {

        private StandardTitanGraph graph=null;
        private boolean provided=false;

        public void setGraph(TitanGraph graph) {
            Preconditions.checkArgument(graph!=null && graph.isOpen(),"Need to provide open graph");
            this.graph = (StandardTitanGraph)graph;
            provided = true;
        }

        public void initializeGraph(Configuration config) {
            if (!provided) {
                this.graph = (StandardTitanGraph) TitanFactory.open((BasicConfiguration) config);
            }
        }

        public void close() {
            if (!provided && null != graph && graph.isOpen()) {
                graph.close();
                graph=null;
            }
        }

        public final StandardTitanGraph get() {
            Preconditions.checkState(graph!=null);
            return graph;
        }


    }

}
