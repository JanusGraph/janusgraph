package org.janusgraph.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import org.janusgraph.core.TitanVertex;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.database.StandardTitanGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.olap.VertexJobConverter;
import org.janusgraph.graphdb.transaction.StandardTitanTx;
import org.janusgraph.graphdb.util.WorkerPool;
import org.janusgraph.graphdb.vertices.PreloadedVertex;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionedVertexProgramExecutor<M> {

    private static final Logger log =
            LoggerFactory.getLogger(PartitionedVertexProgramExecutor.class);

    private final StandardTitanGraph graph;
    private final IDManager idManager;
    private final FulgoraMemory memory;
    private final FulgoraVertexMemory<M> vertexMemory;
    private final VertexProgram<M> vertexProgram;


    public static final String GHOTST_PARTITION_VERTEX = "partition-ghost";
    public static final String PARTITION_VERTEX_POSTSUCCESS = "partition-success";
    public static final String PARTITION_VERTEX_POSTFAIL = "partition-fail";

    public PartitionedVertexProgramExecutor(StandardTitanGraph graph, FulgoraMemory memory,
                                 FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram) {
        this.graph=graph;
        this.idManager = graph.getIDManager();
        this.memory = memory;
        this.vertexMemory = vertexMemory;
        this.vertexProgram = vertexProgram;
    }

    public void run(int numThreads, ScanMetrics metrics) {
        StandardTitanTx tx=null;
        Map<Long,EntryList> pVertexAggregates = vertexMemory.retrievePartitionAggregates();
        if (pVertexAggregates.isEmpty()) return; //Nothing to do here

        try (WorkerPool workers = new WorkerPool(numThreads)) {
            tx = VertexJobConverter.startTransaction(graph);
            for (Map.Entry<Long,EntryList> pvertices : pVertexAggregates.entrySet()) {
                if (pvertices.getValue()==null) {
                    metrics.incrementCustom(GHOTST_PARTITION_VERTEX);
                    continue;
                }
                workers.submit(new PartitionedVertexProcessor(pvertices.getKey(),pvertices.getValue(),tx,metrics));
            }
        } catch (Throwable ex) {
            log.error("Could not post-process partitioned vertices", ex);
            metrics.incrementCustom(PARTITION_VERTEX_POSTFAIL);
        } finally {
            if (tx!=null && tx.isOpen()) tx.rollback();
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
                v.setAccessCheck(PreloadedVertex.OPENSTAR_CHECK);
                v.addToQueryCache(VertexProgramScanJob.SYSTEM_PROPS_QUERY,preloaded);
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
