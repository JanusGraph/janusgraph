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

package org.janusgraph.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.olap.QueryContainer;
import org.janusgraph.graphdb.olap.VertexJobConverter;
import org.janusgraph.graphdb.olap.VertexScanJob;
import org.janusgraph.graphdb.vertices.PreloadedVertex;
import org.janusgraph.util.datastructures.Retriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexMapJob implements VertexScanJob {

    private static final Logger log =
            LoggerFactory.getLogger(VertexMapJob.class);


    public static final PreloadedVertex.AccessCheck MAPREDUCE_CHECK = new PreloadedVertex.AccessCheck() {
        @Override
        public final void accessEdges() {
            throw GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce();
        }

        @Override
        public final void accessProperties() { }

        @Override
        public void accessSetProperty() {
            throw GraphComputer.Exceptions.vertexPropertiesCanNotBeUpdatedInMapReduce();
        }

        @Override
        public Retriever<SliceQuery, EntryList> retrieveSliceQuery() {
            return PreloadedVertex.EMPTY_RETRIEVER;
        }
    };

    private final IDManager idManager;
    private final Map<MapReduce, FulgoraMapEmitter> mapJobs;
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
        Map<MapReduce, FulgoraMapEmitter> cloneMap = new HashMap<>(mapJobs.size());
        for (Map.Entry<MapReduce, FulgoraMapEmitter> entry : mapJobs.entrySet()) {
            cloneMap.put(entry.getKey().clone(), entry.getValue());
        }
        return new VertexMapJob(idManager, vertexMemory, cloneMap);
    }

    @Override
    public void workerIterationStart(JanusGraph graph, Configuration config, ScanMetrics metrics) {
        for (Map.Entry<MapReduce, FulgoraMapEmitter> mapJob : mapJobs.entrySet()) {
            mapJob.getKey().workerStart(MapReduce.Stage.MAP);
        }
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        for (Map.Entry<MapReduce, FulgoraMapEmitter> mapJob : mapJobs.entrySet()) {
            mapJob.getKey().workerEnd(MapReduce.Stage.MAP);
        }
    }

    @Override
    public void process(JanusGraphVertex vertex, ScanMetrics metrics) {
        final PreloadedVertex v = (PreloadedVertex) vertex;
        if (vertexMemory != null) {
            VertexMemoryHandler vh = new VertexMemoryHandler(vertexMemory, v);
            v.setPropertyMixing(vh);
        }
        v.setAccessCheck(MAPREDUCE_CHECK);
        if (idManager.isPartitionedVertex(v.longId()) && !idManager.isCanonicalVertexId(v.longId())) {
            return; //Only consider the canonical partition vertex representative
        }
        for (Map.Entry<MapReduce, FulgoraMapEmitter> mapJob : mapJobs.entrySet()) {
            final MapReduce job = mapJob.getKey();
            try {
                job.map(v, mapJob.getValue());
                metrics.incrementCustom(MAP_JOB_SUCCESS);
            } catch (Throwable ex) {
                log.error("Encountered exception executing map job [" + job + "] on vertex [" + vertex + "]:", ex);
                metrics.incrementCustom(MAP_JOB_FAILURE);
            }
        }
    }

    @Override
    public void getQueries(QueryContainer queries) {

    }

    public static Executor getVertexMapJob(StandardJanusGraph graph, FulgoraVertexMemory vertexMemory,
                                           Map<MapReduce, FulgoraMapEmitter> mapJobs) {
        VertexMapJob job = new VertexMapJob(graph.getIDManager(), vertexMemory, mapJobs);
        for (Map.Entry<MapReduce, FulgoraMapEmitter> mapJob : mapJobs.entrySet()) {
            mapJob.getKey().workerStart(MapReduce.Stage.MAP);
        }
        return new Executor(graph, job);
    }

    public static class Executor extends VertexJobConverter implements Closeable {

        private Executor(JanusGraph graph, VertexMapJob job) {
            super(graph, job);
            open(this.graph.get().getConfiguration().getConfiguration());
        }

        private Executor(final Executor copy) {
            super(copy);
            open(this.graph.get().getConfiguration().getConfiguration());
        }

        @Override
        public List<SliceQuery> getQueries() {
            List<SliceQuery> queries = super.getQueries();
            queries.add(VertexProgramScanJob.SYSTEM_PROPS_QUERY);
            return queries;
        }

        @Override
        public void workerIterationStart(Configuration jobConfig, Configuration graphConfig, ScanMetrics metrics) {
            job.workerIterationStart(graph.get(), jobConfig, metrics);
        }

        @Override
        public void workerIterationEnd(ScanMetrics metrics) {
            job.workerIterationEnd(metrics);
        }

        @Override
        public Executor clone() {
            return new Executor(this);
        }

        @Override
        public void close() {
            super.close();
        }

    }

}


