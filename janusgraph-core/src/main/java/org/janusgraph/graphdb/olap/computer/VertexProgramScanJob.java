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
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.vertices.PreloadedVertex;

import java.io.Closeable;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexProgramScanJob<M> implements VertexScanJob {

    private static final MessageScope.Global globalScope = MessageScope.Global.instance();
    //Held by every clone of a program, of any job, as a clone may number the steps of a copied traversal, see clone()
    private static final Object STEP_NUMBERING_LOCK = new Object();
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

    /*
     * The processors of a scan may clone the job at the same time, each for a work block of its own. A
     * TraversalVertexProgram's clone applies the traversal's strategies to a copy of the traversal, which numbers its
     * steps with the StepPosition that every copy of a TinkerPop traversal shares with the traversal it was copied
     * from, and the program of a job whose traversal can't be serialized is handed the traversal itself, which other
     * jobs may share. Copies numbered at the same time get mixed-up step ids, by which a clone finds a barrier in the
     * job's memory and the step of each traverser it receives: it fails, or sends a traverser to another step. So
     * programs are cloned one at a time, those of every kind, as any program may copy a traversal in its clone. A
     * processor clones the job only when it starts a work block, never while it processes a vertex. TinkerPop also
     * numbers the steps of a program's own copy of the traversal while it makes the program, before the job starts and
     * out of this lock's reach, so jobs whose programs share a traversal may still mix up ids when one of them starts
     * while another one runs; only TinkerPop can stop its copies sharing the StepPosition.
     */
    @Override
    public VertexProgramScanJob<M> clone() {
        final VertexProgram<M> program;
        synchronized (STEP_NUMBERING_LOCK) {
            program = this.vertexProgram.clone();
        }
        return new VertexProgramScanJob<>(this.idManager, this.memory, this.vertexMemory, program);
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
        Object vertexId = v.id();
        VertexMemoryHandler<M> vh = new VertexMemoryHandler(vertexMemory,v);
        vh.setInExecute(true);
        v.setAccessCheck(PreloadedVertex.OPENSTAR_CHECK);
        if (idManager.isPartitionedVertex(vertexId)) {
            if (idManager.isCanonicalVertexId(((Number) vertexId).longValue())) {
                EntryList results = v.getFromCache(SYSTEM_PROPS_QUERY);
                if (results == null) results = EntryList.EMPTY_LIST;
                vertexMemory.setLoadedProperties(((Number) vertexId).longValue(), results);
            }
            for (MessageScope scope : vertexMemory.getPreviousScopes()) {
                if (scope instanceof MessageScope.Local) {
                    vh.receiveMessages(scope)
                      .iterator()
                      .forEachRemaining(m -> vertexMemory.aggregateMessage(((Number) vertexId).longValue(), m, scope));
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

        //The transactions of the clones which the scanner's processors work with, one per processor and work block.
        //The elements read in them outlive the clones' work, in messages, in the memory and in the results, so they end
        //only when this executor closes, after the computer's job
        private final Queue<StandardJanusGraphTx> cloneTransactions;
        //Whether the executor has closed, shared with the clones like the queue
        private final AtomicBoolean closed;

        private Executor(JanusGraph graph, VertexProgramScanJob job) {
            super(graph, job);
            open(this.graph.get().getConfiguration().getConfiguration());
            cloneTransactions = new ConcurrentLinkedQueue<>();
            closed = new AtomicBoolean();
        }

        private Executor(final Executor copy) {
            super(copy);
            open(this.graph.get().getConfiguration().getConfiguration());
            cloneTransactions = copy.cloneTransactions;
            closed = copy.closed;
            cloneTransactions.add(tx);
            //Cloned by a processor which outlived the scan, after the close: the transaction is this clone's to roll
            //back if the close's rollback hasn't taken it from the queue, and the close's otherwise
            if (closed.get() && cloneTransactions.remove(tx)) {
                tx.rollback();
            }
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
            closed.set(true);
            try {
                rollbackAll(cloneTransactions);
            } finally {
                super.close();
            }
        }

    }
}


