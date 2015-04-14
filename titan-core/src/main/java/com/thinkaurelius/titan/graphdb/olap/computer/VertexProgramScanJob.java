package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanVertexStep;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexProgramScanJob<M> implements VertexScanJob {

    private static final Logger log =
            LoggerFactory.getLogger(VertexProgramScanJob.class);

    private final IDManager idManager;
    private final FulgoraMemory memory;
    private final FulgoraVertexMemory<M> vertexMemory;
    private final VertexProgram<M> vertexProgram;

    private final MessageCombiner<M> combiner;

    private VertexProgramScanJob(IDManager idManager, FulgoraMemory memory,
                                FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram) {
        this.idManager = idManager;
        this.memory = memory;
        this.vertexMemory = vertexMemory;
        this.vertexProgram = vertexProgram;
        this.combiner = FulgoraUtil.getMessageCombiner(vertexProgram);
    }

    @Override
    public VertexProgramScanJob<M> clone() {
        return new VertexProgramScanJob<>(this.idManager, this.memory, this.vertexMemory, this.vertexProgram
                .clone());
    }

    @Override
    public void workerIterationStart(TitanGraph graph, Configuration config, ScanMetrics metrics) {
        vertexProgram.workerIterationStart(memory); //TODO: Should this also be immutable?
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        vertexProgram.workerIterationEnd(memory.asImmutable());
    }

    @Override
    public void process(TitanVertex vertex, ScanMetrics metrics) {
        PreloadedVertex v = (PreloadedVertex)vertex;
        long vertexId = v.longId();
        VertexMemoryHandler<M> vh = new VertexMemoryHandler(vertexMemory,v);
        v.setExceptionOnRetrieve(true);
        if (idManager.isPartitionedVertex(vertexId)) {
            if (idManager.isCanonicalVertexId(vertexId)) {
                EntryList results = v.getFromCache(SYSTEM_PROPS_QUERY);
                if (results == null) results = EntryList.EMPTY_LIST;
                vertexMemory.setLoadedProperties(vertexId,results);
            }
            for (MessageScope scope : vertexMemory.getPreviousScopes()) {
                if (scope instanceof MessageScope.Local) {
                    M combinedMsg = null;
                    for (Iterator<M> msgIter = vh.receiveMessages(scope); msgIter.hasNext(); ) {
                        M msg = msgIter.next();
                        if (combinedMsg==null) combinedMsg=msg;
                        else combinedMsg = combiner.combine(combinedMsg,msg);
                    }
                    if (combinedMsg!=null) vertexMemory.aggregateMessage(vertexId,combinedMsg,scope);
                }
            }
        } else {
            v.setPropertyMixing(vh);
            vertexProgram.execute(v, vh, memory);
        }
    }

    @Override
    public void getQueries(QueryContainer queries) {
        if (vertexProgram instanceof TraversalVertexProgram) {
            //TraversalVertexProgram currently makes the assumption that the entire star-graph around a vertex
            //is available (in-memory). Hence, this special treatment here.
            //TODO: After TraversalVertexProgram is adjusted, remove this
            queries.addQuery().direction(Direction.BOTH).edges();
            return;
        }

        for (MessageScope scope : vertexMemory.getPreviousScopes()) {
            if (scope instanceof MessageScope.Global) {
                queries.addQuery().direction(Direction.BOTH).edges();
            } else {
                assert scope instanceof MessageScope.Local;
                TitanVertexStep<Vertex> startStep = FulgoraUtil.getReverseTitanVertexStep((MessageScope.Local) scope,queries.getTransaction());
                QueryContainer.QueryBuilder qb = queries.addQuery();
                startStep.makeQuery(qb);
                qb.edges();
            }
        }
    }


    public static<M> Executor getVertexProgramScanJob(StandardTitanGraph graph, FulgoraMemory memory,
                                                  FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram) {
        VertexProgramScanJob<M> job = new VertexProgramScanJob<M>(graph.getIDManager(),memory,vertexMemory,vertexProgram);
        return new Executor(graph,job);
    }

    //Query for all system properties+edges and normal properties
    static final SliceQuery SYSTEM_PROPS_QUERY = new SliceQuery(
            IDHandler.getBounds(RelationCategory.PROPERTY, true)[0],
            IDHandler.getBounds(RelationCategory.PROPERTY,false)[1]);

    public static class Executor extends VertexJobConverter {

        private Executor(TitanGraph graph, VertexProgramScanJob job) {
            super(graph, job);
        }

        private Executor(final Executor copy) {
            super(copy);
        }

        @Override
        public List<SliceQuery> getQueries() {
            List<SliceQuery> queries = super.getQueries();
            queries.add(SYSTEM_PROPS_QUERY);
            return queries;
        }

        @Override
        public void workerIterationEnd(ScanMetrics metrics) {
            super.workerIterationEnd(metrics);
        }

        @Override
        public Executor clone() { return new Executor(this); }


    }





}


