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

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.ReadOnlyTransactionException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.olap.QueryContainer;
import org.janusgraph.graphdb.olap.VertexJobConverter;
import org.janusgraph.graphdb.olap.VertexScanJob;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.vertices.PreloadedVertex;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexProgramScanJob<M> implements VertexScanJob {

    private static final MessageScope.Global globalScope = MessageScope.Global.instance();
    private final IDManager idManager;
    private final FulgoraMemory memory;
    private final FulgoraVertexMemory<M> vertexMemory;
    private final VertexProgram<M> vertexProgram;

    private VertexProgramScanJob(IDManager idManager, FulgoraMemory memory,
                                FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram) {
        this.idManager = idManager;
        this.memory = memory;
        this.vertexMemory = vertexMemory;
        this.vertexProgram = vertexProgram;
    }

    @Override
    public VertexProgramScanJob<M> clone() {
        return new VertexProgramScanJob<>(this.idManager, this.memory, this.vertexMemory, this.vertexProgram
                .clone());
    }

    @Override
    public void workerIterationStart(JanusGraph graph, Configuration config, ScanMetrics metrics) {
        vertexProgram.workerIterationStart(memory.asImmutable());
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        vertexProgram.workerIterationEnd(memory.asImmutable());
    }

    @Override
    public void process(JanusGraphVertex vertex, ScanMetrics metrics) {
        PreloadedVertex v = (PreloadedVertex)vertex;
        long vertexId = v.longId();
        VertexMemoryHandler<M> vh = new VertexMemoryHandler(vertexMemory,v);
        vh.setInExecute(true);
        v.setAccessCheck(PreloadedVertex.OPENSTAR_CHECK);
        if (idManager.isPartitionedVertex(vertexId)) {
            if (idManager.isCanonicalVertexId(vertexId)) {
                EntryList results = v.getFromCache(SYSTEM_PROPS_QUERY);
                if (results == null) results = EntryList.EMPTY_LIST;
                vertexMemory.setLoadedProperties(vertexId,results);
            }
            for (MessageScope scope : vertexMemory.getPreviousScopes()) {
                if (scope instanceof MessageScope.Local) {
                    vh.receiveMessages(scope)
                      .iterator()
                      .forEachRemaining(m -> vertexMemory.aggregateMessage(vertexId, m, scope));
                }
            }
        } else {
            v.setPropertyMixing(vh);
            try {
                vertexProgram.execute(v, vh, memory);
            } catch (ReadOnlyTransactionException e) {
                // Ignore read-only transaction errors in FulgoraGraphComputer. In testing these errors are associated
                // with cleanup of TraversalVertexProgram.HALTED_TRAVERSALS properties which can safely remain in graph.
            }
        }
        vh.setInExecute(false);
    }

    @Override
    public void getQueries(QueryContainer queries) {
        Set<MessageScope> previousScopes = vertexMemory.getPreviousScopes();
        if (vertexProgram instanceof TraversalVertexProgram || vertexProgram instanceof ShortestPathVertexProgram ||
            vertexProgram instanceof ConnectedComponentVertexProgram || previousScopes.contains(globalScope)) {
            //TraversalVertexProgram currently makes the assumption that the entire star-graph around a vertex
            //is available (in-memory). Hence, this special treatment here.
            //TODO: After TraversalVertexProgram is adjusted, remove this
            //Special handling for ShortestPathVertexProgram necessary until TINKERPOP-2187 is resolved
            //Apparently, ConnectedComponentVertexProgram also needs this special handling
            queries.addQuery().direction(Direction.BOTH).edges();
        }

        for (MessageScope scope : previousScopes) {
            if (scope instanceof MessageScope.Local) {
                JanusGraphVertexStep<Vertex> startStep =
                    FulgoraUtil.getReverseJanusGraphVertexStep((MessageScope.Local) scope, queries.getTransaction());
                QueryContainer.QueryBuilder qb = queries.addQuery();
                startStep.makeQuery(qb);
                qb.edges();
            }
        }
    }

    public static<M> Executor getVertexProgramScanJob(StandardJanusGraph graph, FulgoraMemory memory,
                                                  FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram) {
        final VertexProgramScanJob<M> job = new VertexProgramScanJob<>(graph.getIDManager(), memory, vertexMemory, vertexProgram);
        return new Executor(graph,job);
    }

    //Query for all system properties+edges and normal properties
    static final SliceQuery SYSTEM_PROPS_QUERY = new SliceQuery(
            IDHandler.getBounds(RelationCategory.PROPERTY, true)[0],
            IDHandler.getBounds(RelationCategory.PROPERTY,false)[1]);

    public static class Executor extends VertexJobConverter implements Closeable {

        private Executor(JanusGraph graph, VertexProgramScanJob job) {
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
            queries.add(SYSTEM_PROPS_QUERY);
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
        public Executor clone() { return new Executor(this); }

        @Override
        public void close() {
            super.close();
        }

    }
}


