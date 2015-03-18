package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexMapJob implements VertexScanJob {

    private static final Logger log =
            LoggerFactory.getLogger(VertexMapJob.class);

    private final IDManager idManager;
    private final Map<MapReduce,FulgoraMapEmitter> mapJobs;
    private final FulgoraVertexMemory vertexMemory;

    public static final String MAP_JOB_SUCCESS = "map-success";
    public static final String MAP_JOB_FAILURE = "map-fail";

    private VertexMapJob(IDManager idManager, FulgoraVertexMemory vertexMemory,
                            Map<MapReduce, FulgoraMapEmitter> mapJobs) {
        this.mapJobs = mapJobs;
        this.vertexMemory = vertexMemory;
        this.idManager = idManager;
    }

    @Override
    public VertexMapJob clone() {
        ImmutableMap.Builder<MapReduce,FulgoraMapEmitter> cloneMap = ImmutableMap.builder();
        for (Map.Entry<MapReduce,FulgoraMapEmitter> entry : mapJobs.entrySet()) {
            try {
                cloneMap.put(entry.getKey().clone(),entry.getValue());
            } catch (CloneNotSupportedException e) {
                throw new AssertionError("MapReduce implementations need to support clone()",e);
            }
        }
        return new VertexMapJob(idManager,vertexMemory,cloneMap.build());
    }

    @Override
    public void process(TitanVertex vertex, ScanMetrics metrics) {
        PreloadedVertex v = (PreloadedVertex)vertex;
        if (vertexMemory!=null) {
            VertexMemoryHandler vh = new VertexMemoryHandler(vertexMemory,v);
            v.setPropertyMixing(vh);
        }
        v.setExceptionOnRetrieve(true);
        if (idManager.isPartitionedVertex(v.longId()) && !idManager.isCanonicalVertexId(v.longId())) {
            return; //Only consider the canonical partition vertex representative
        } else {
            for (Map.Entry<MapReduce,FulgoraMapEmitter> mapJob : mapJobs.entrySet()) {
                MapReduce job = mapJob.getKey();
                try {
                    job.map(v,mapJob.getValue());
                    metrics.incrementCustom(MAP_JOB_SUCCESS);
                } catch (Throwable ex) {
                    log.error("Encountered exception executing map job ["+job+"] on vertex ["+vertex+"]:",ex);
                    metrics.incrementCustom(MAP_JOB_FAILURE);
                }
            }
        }
    }

    @Override
    public void getQueries(QueryContainer queries) {

    }

    public static Executor getVertexMapJob(StandardTitanGraph graph, FulgoraVertexMemory vertexMemory,
                                              Map<MapReduce,FulgoraMapEmitter> mapJobs) {
        VertexMapJob job = new VertexMapJob(graph.getIDManager(),vertexMemory,mapJobs);
        return new Executor(graph,job);
    }

    public static class Executor extends VertexJobConverter {

        private Executor(TitanGraph graph, VertexMapJob job) {
            super(graph, job);
        }

        private Executor(final Executor copy) { super(copy); }

        @Override
        public List<SliceQuery> getQueries() {
            List<SliceQuery> queries = super.getQueries();
            queries.add(VertexProgramScanJob.SYSTEM_PROPS_QUERY);
            return queries;
        }

        @Override
        public Executor clone() { return new Executor(this); }

    }

}


