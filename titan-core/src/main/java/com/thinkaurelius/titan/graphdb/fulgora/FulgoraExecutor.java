package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractFuture;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.olap.OLAPJob;
import com.thinkaurelius.titan.core.olap.StateInitializer;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.QueryExecutor;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQuery;
import com.thinkaurelius.titan.graphdb.relations.CacheEdge;
import com.thinkaurelius.titan.graphdb.relations.CacheProperty;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.VertexFactory;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.blueprints.Direction;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
class FulgoraExecutor<S> extends AbstractFuture<Map<Long,S>> implements Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(FulgoraExecutor.class);

    private static final int QUEUE_SZIE = 1000;
    private static final int TIMEOUT_MS = 60000; // 60 seconds
    private static final int NUM_VERTEX_DEFAULT = 10000;

    private final SliceQuery[] queries;
    private final List<BlockingQueue<QueryResult>> dataQueues;
    private final Thread[] pullThreads;
    private final ExecutorService processor;
    private final StandardTitanTx tx;
    private final OLAPJob job;

    final ConcurrentMap<Long,S> vertexStates;
    final String stateKey;
    final StateInitializer<S> initializer;

    private boolean processingException = false;

    FulgoraExecutor(final List<SliceQuery> sliceQueries, final StandardTitanTx tx,
                    final int numVertices, final int numProcessors,
                    final String stateKey, final OLAPJob job,
                    final StateInitializer<S> initializer, final Map<Long,S> initialState) {
        this.tx=tx;
        this.stateKey = stateKey;
        this.job = job;
        this.initializer = initializer;
        this.queries = sliceQueries.toArray(new SliceQuery[sliceQueries.size()]);
        BackendTransaction btx = tx.getTxHandle();

        dataQueues = new ArrayList<BlockingQueue<QueryResult>>(sliceQueries.size());
        pullThreads = new Thread[sliceQueries.size()];
        for (int i = 0; i < sliceQueries.size(); i++) {
            SliceQuery sq = queries[i];
            BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<QueryResult>(QUEUE_SZIE);
            dataQueues.add(queue);
            pullThreads[i]=new Thread(new DataPuller(queue,btx.edgeStoreKeys(sq)));
            pullThreads[i].start();
        }
        //Prepare vertex state
        if (initialState!=null && !initialState.isEmpty() && initialState instanceof ConcurrentMap && initialState.size()>= numVertices) {
            vertexStates = (ConcurrentMap<Long, S>) initialState;
        } else {
            vertexStates = new NonBlockingHashMapLong<S>(numVertices>0?numVertices:NUM_VERTEX_DEFAULT);
            if (initialState!=null && !initialState.isEmpty()) {
                for (Map.Entry<Long,S> entry : initialState.entrySet())
                    vertexStates.put(entry.getKey(),entry.getValue());
            }
        }

        processor = new ThreadPoolExecutor(numProcessors, numProcessors, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(QUEUE_SZIE));
    }

    S getVertexState(long vertexId) {
        S state = vertexStates.get(vertexId);
        if (state==null) {
            state = initializer.initialState();
            vertexStates.put(vertexId,state);
        }
        return state;
    }

    void setVertexState(long vertexId, S state) {
        vertexStates.put(vertexId,state);
    }

    private boolean allPullingThreadsDone() {
        for (int i = 0; i < pullThreads.length; i++) {
            if (pullThreads[i].isAlive()) return false;
        }
        return true;
    }

    @Override
    public void run() {
        try {
            mainloop:
            while (true) {
                if (dataQueues.get(0).isEmpty()) {
                    if (allPullingThreadsDone() && dataQueues.get(0).isEmpty()) break mainloop;
                }
                FulgoraVertex vertex = null;
                for (int i = 0; i < queries.length; i++) {
                    BlockingQueue<QueryResult> queue = dataQueues.get(i);
                    QueryResult qr = queue.poll(TIMEOUT_MS,TimeUnit.MILLISECONDS);
                    if (qr==null) {
                        if (vertex==null && allPullingThreadsDone()) break mainloop;
                        throw new TitanException("Timed out waiting for next vertex data - storage error likely");
                    }
                    assert qr!=null;
                    if (vertex==null) vertex = new FulgoraVertex(tx,qr.vertexId,this);
                    else Preconditions.checkState(vertex.getID()==qr.vertexId,"Vertex id mismatch %s<>%s",vertex.getID(),qr.vertexId);
                    vertex.addToQueryCache(queries[i],qr.entries);
                }
                assert vertex!=null;
                processor.submit(new VertexProcessor<S>(vertex));
            }
            processor.shutdown();
            processor.awaitTermination(TIMEOUT_MS,TimeUnit.MILLISECONDS);
            if (!processor.isTerminated()) throw new TitanException("Timed out waiting for vertex processors");
            set(vertexStates);
        } catch (Throwable e) {
            log.error("Exception occured during job execution: {}",e);
            setException(e);
        } finally {
            processor.shutdownNow();
        }
    }

    final QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery> edgeProcessor = new QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery>() {

        @Override
        public Iterator<TitanRelation> getNew(VertexCentricQuery query) {
            return Iterators.emptyIterator();
        }

        @Override
        public boolean hasDeletions(VertexCentricQuery query) {
            return false;
        }

        @Override
        public boolean isDeleted(VertexCentricQuery query, TitanRelation result) {
            return false;
        }

        @Override
        public Iterator<TitanRelation> execute(VertexCentricQuery query, SliceQuery sq, Object exeInfo) {
            assert exeInfo==null;

            final InternalVertex vertex = query.getVertex();

            Iterable<Entry> iter = vertex.loadRelations(sq, new Retriever<SliceQuery, EntryList>() {
                @Override
                public EntryList get(SliceQuery query) {
                    return StaticArrayEntryList.EMPTY_LIST;
                }
            });

            return Iterables.transform(iter, new Function<Entry, TitanRelation>() {
                @Override
                public TitanRelation apply(@Nullable Entry data) {
                    return RelationConstructor.readRelation(vertex,data,tx.getEdgeSerializer(),tx,neighborVertices);
                }
            }).iterator();
        }
    };

    private final VertexFactory neighborVertices = new VertexFactory() {
        @Override
        public InternalVertex getExistingVertex(long id) {
            return new FulgoraNeighborVertex(id,FulgoraExecutor.this);
        }
    };


    private class VertexProcessor<S> implements Runnable {

        final FulgoraVertex vertex;

        private VertexProcessor(FulgoraVertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public void run() {
            try {
                job.process(vertex);
            } catch (Throwable e) {
                log.error("Exception processing vertex ["+vertex.getID()+"]: ",e);
                processingException = true;
                vertexStates.put(vertex.getID(),null); //Invalidate state
            }

        }
    }

    private static class DataPuller implements Runnable {

        private final BlockingQueue<QueryResult> queue;
        private final KeyIterator keyIter;

        private DataPuller(BlockingQueue<QueryResult> queue, KeyIterator keyIter) {
            this.queue = queue;
            this.keyIter = keyIter;
        }

        @Override
        public void run() {
            try {
                while (keyIter.hasNext()) {
                    StaticBuffer key = keyIter.next();
                    RecordIterator<Entry> entries = keyIter.getEntries();
                    long vertexId = IDHandler.getKeyID(key);
                    EntryList entryList = StaticArrayEntryList.ofStaticBuffer(entries, StaticArrayEntry.ENTRY_GETTER);
                    try {
                        queue.put(new QueryResult(vertexId,entryList));
                    } catch (InterruptedException e) {
                        log.error("Data-pulling thread interrupted while waiting on queue",e);
                        break;
                    }
                }
            } catch (Throwable e) {
                log.error("Could not load data from storage: {}",e);
            } finally {
                try {
                    keyIter.close();
                } catch (IOException e) {
                    log.warn("Could not close storage iterator ", e);
                }
            }
        }
    }

    private static class QueryResult {

        final EntryList entries;
        final long vertexId;

        private QueryResult(long vertexId, EntryList entries) {
            this.entries = entries;
            this.vertexId = vertexId;
        }
    }

}
