package com.thinkaurelius.titan.graphdb.olap;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
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

    private final StandardTitanTx tx;
    private final VertexScanJob job;
    private final IDManager idManager;

    private VertexJobConverter(StandardTitanTx tx, VertexScanJob job) {
        this.tx = tx;
        this.job = job;
        this.idManager = tx.getIdInspector();
    }

    public static ScanJob convert(StandardTitanTx tx, VertexScanJob vertexJob) {
        return new VertexJobConverter(tx,vertexJob);
    }

    @Override
    public void setup(Configuration config, ScanMetrics metrics) {
        job.setup(config,metrics);
    }

    @Override
    public void teardown(ScanMetrics metrics) {
        job.teardown(metrics);
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
