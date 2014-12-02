package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanElementTraversal;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanVertexStep;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final StandardTitanGraph graph;
    private final IDManager idManager;
    private final FulgoraMemory memory;
    private final FulgoraVertexMemory<M> vertexMemory;
    private final VertexProgram<M> vertexProgram;

    private final MessageCombiner<M> combiner;
    private final Set<MessageScope> scopes;

    public static final String GHOTST_PARTITION_VERTEX = "partition-ghost";
    public static final String PARTITION_VERTEX_POSTSUCCESS = "partition-success";
    public static final String PARTITION_VERTEX_POSTFAIL = "partition-fail";

    private VertexProgramScanJob(StandardTitanGraph graph, FulgoraMemory memory,
                                FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram) {
        this.graph = graph;
        this.idManager = graph.getIDManager();
        this.memory = memory;
        this.vertexMemory = vertexMemory;
        this.vertexProgram = vertexProgram;
        this.combiner = FulgoraUtil.getMessageCombiner(vertexProgram);
        this.scopes = vertexProgram.getMessageScopes(memory);

    }

    @Override
    public void process(TitanVertex vertex, ScanMetrics metrics) {
        PreloadedVertex v = (PreloadedVertex)vertex;
        long vertexId = v.longId();
        VertexMemoryHandler<M> vh = new VertexMemoryHandler(vertexMemory,v);
        v.setPropertyMixing(vh);
        v.setExceptionOnRetrieve(true);
        if (idManager.isPartitionedVertex(vertexId)) {
            if (idManager.isCanonicalVertexId(vertexId)) {
                EntryList results = v.getFromCache(SYSTEM_PROPS_QUERY);
                if (results == null) results = EntryList.EMPTY_LIST;
                vertexMemory.setLoadedProperties(vertexId,results);
            }
            for (MessageScope scope : scopes) {
                if (scope instanceof MessageScope.Local) {
                    M combinedMsg = null;
                    for (M msg : vh.receiveMessages(scope)) {
                        if (combinedMsg==null) combinedMsg=msg;
                        else combinedMsg = combiner.combine(combinedMsg,msg);
                    }
                    if (combinedMsg!=null) vertexMemory.aggregateMessage(vertexId,combinedMsg,scope);
                }
            }
        } else {
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
                FulgoraElementTraversal<Vertex,Edge> incident = FulgoraUtil.getTitanTraversal((MessageScope.Local) scope,queries.getTransaction());
                incident.applyStrategies(TraversalEngine.COMPUTER);
                TitanVertexStep<Vertex> startStep = (TitanVertexStep<Vertex>) TraversalHelper.getStart(incident);
                startStep.reverse();
                QueryContainer.QueryBuilder qb = queries.addQuery();
                startStep.makeQuery(qb);
                qb.edges();
            }
        }
    }


    public static<M> Executor getVertexProgramScanJob(StandardTitanGraph graph, FulgoraMemory memory,
                                                  FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram,
                                                  int numThreads) {
        VertexProgramScanJob<M> job = new VertexProgramScanJob<M>(graph,memory,vertexMemory,vertexProgram);
        return new Executor(graph,job,numThreads);
    }

    //Query for all system properties+edges and normal properties
    static final SliceQuery SYSTEM_PROPS_QUERY = new SliceQuery(
            IDHandler.getBounds(RelationCategory.PROPERTY, true)[0],
            IDHandler.getBounds(RelationCategory.PROPERTY,false)[1]);

    public static class Executor extends VertexJobConverter {

        private final int numThreads;

        private Executor(TitanGraph graph, VertexProgramScanJob job, int numThreads) {
            super(graph, job);
            Preconditions.checkArgument(numThreads>0,"Invalid number: %s",numThreads);
            this.numThreads = numThreads;
        }

        @Override
        public List<SliceQuery> getQueries() {
            List<SliceQuery> queries = super.getQueries();
            queries.add(SYSTEM_PROPS_QUERY);
            return queries;
        }

        @Override
        public void teardown(ScanMetrics metrics) {
            ThreadPoolExecutor processor = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(128));
            processor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            try {
                ((VertexProgramScanJob)job).processPartitionedVertices(processor,tx,metrics);
                processor.shutdown();
                processor.awaitTermination(10,TimeUnit.SECONDS);
                if (!processor.isTerminated()) log.error("Processor did not terminate in time");
            } catch (Throwable ex) {
                log.error("Could not post-process partitioned vertices",ex);
                metrics.incrementCustom(PARTITION_VERTEX_POSTFAIL);
            } finally {
                processor.shutdownNow();
            }
            super.teardown(metrics);
        }

    }

    private void processPartitionedVertices(final ExecutorService exeuctor, final StandardTitanTx tx, ScanMetrics metrics) {
        Map<Long,EntryList> pVertexAggregates = vertexMemory.retrievePartitionAggregates();
        for (Map.Entry<Long,EntryList> pvertices : pVertexAggregates.entrySet()) {
            if (pvertices.getValue()==null) {
                metrics.incrementCustom(GHOTST_PARTITION_VERTEX);
                continue;
            }
            exeuctor.submit(new PartitionedVertexProcessor(pvertices.getKey(),pvertices.getValue(),tx,metrics));
        }
    }

    private class PartitionedVertexProcessor implements Runnable {

        private final long vertexId;
        private final EntryList preloaded;
        private final StandardTitanTx tx;
        private final ScanMetrics metrics;

        private PartitionedVertexProcessor(long vertexId, EntryList preloaded, StandardTitanTx tx, ScanMetrics metrics) {
            Preconditions.checkArgument(idManager.isPartitionedVertex(vertexId) && idManager.isCanonicalVertexId(vertexId));
            assert preloaded!=null;
            this.vertexId = vertexId;
            this.preloaded = preloaded;
            this.tx = tx;
            this.metrics = metrics;
        }

        @Override
        public void run() {
            try {
                TitanVertex vertex = tx.getInternalVertex(vertexId);
                Preconditions.checkArgument(vertex instanceof PreloadedVertex,
                        "The bounding transaction is not configured correctly");
                PreloadedVertex v = (PreloadedVertex)vertex;
                v.setExceptionOnRetrieve(true);
                v.addToQueryCache(SYSTEM_PROPS_QUERY,preloaded);
                VertexMemoryHandler.Partition<M> vh = new VertexMemoryHandler.Partition<M>(vertexMemory,v);
                v.setPropertyMixing(vh);
                vertexProgram.execute(v,vh,memory);
                metrics.incrementCustom(PARTITION_VERTEX_POSTSUCCESS);
            } catch (Throwable e) {
                metrics.incrementCustom(PARTITION_VERTEX_POSTFAIL);
                log.error("Error post-processing partition vertex: " + vertexId,e);
            }
        }
    }



}


