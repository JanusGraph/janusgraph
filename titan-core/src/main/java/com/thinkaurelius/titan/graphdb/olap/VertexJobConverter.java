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
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexJobConverter implements ScanJob {

    private static final SliceQuery VERTEX_EXISTS_QUERY = new SliceQuery(BufferUtil.zeroBuffer(4),BufferUtil.oneBuffer(4)).setLimit(1);

    private StandardTitanGraph graph;
    private final boolean graphProvided;
    private final VertexScanJob job;

    private StandardTitanTx tx;
    private IDManager idManager;

    private VertexJobConverter(TitanGraph graph, VertexScanJob job) {
        Preconditions.checkArgument(job!=null);
        this.graph = (StandardTitanGraph)graph;
        this.job = job;
        this.graphProvided = (graph!=null);
    }

    public static ScanJob convert(TitanGraph graph, VertexScanJob vertexJob) {
        return new VertexJobConverter(graph,vertexJob);
    }

    public static ScanJob convert(VertexScanJob vertexJob) {
        return new VertexJobConverter(null,vertexJob);
    }

    @Override
    public void setup(Configuration config, ScanMetrics metrics) {
        if (!graphProvided) this.graph = (StandardTitanGraph) TitanFactory.open((BasicConfiguration) config);
        idManager = graph.getIDManager();
        StandardTransactionBuilder txb = graph.buildTransaction().readOnly();
        txb.hasPreloadedData();
        txb.checkInternalVertexExistence(false);
        try {
            tx = (StandardTitanTx)txb.start();
            job.setup(graph, config, metrics);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    private void close() {
        if (null != tx && tx.isOpen())
            tx.rollback();
        if (!graphProvided && null != graph && graph.isOpen())
            graph.close();
    }

    @Override
    public void teardown(ScanMetrics metrics) {
        job.teardown(metrics);
        close();
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        TitanVertex vertex = tx.getInternalVertex(getVertexId(key));
        Preconditions.checkArgument(vertex instanceof PreloadedVertex,
                "The bounding transaction is not configured correctly");
        PreloadedVertex v = (PreloadedVertex)vertex;
        for (Map.Entry<SliceQuery,EntryList> entry : entries.entrySet()) {
            v.addToQueryCache(entry.getKey(),entry.getValue());
        }
        job.process(v,metrics);
    }

    @Override
    public List<SliceQuery> getQueries() {
        QueryContainer qc = new QueryContainer(tx);
        job.getQueries(qc);

        List<SliceQuery> slices = new ArrayList<>();
        slices.add(VERTEX_EXISTS_QUERY);
        slices.addAll(qc.getSliceQueries());
        return slices;
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        return buffer -> {
            long vertexId = getVertexId(buffer);
            if (IDManager.VertexIDType.Invisible.is(vertexId)) return false;
            else return true;
        };
    }

    private long getVertexId(StaticBuffer key) {
        return idManager.getKeyID(key);
    }

}
