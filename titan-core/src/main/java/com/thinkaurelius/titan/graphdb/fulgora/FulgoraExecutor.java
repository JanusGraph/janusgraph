package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.olap.OLAPJob;
import com.thinkaurelius.titan.core.olap.OLAPResult;
import com.thinkaurelius.titan.core.olap.StateInitializer;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.VertexFactory;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
class FulgoraExecutor<S> extends AbstractFuture<OLAPResult<S>> implements Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(FulgoraExecutor.class);

    private static final int QUEUE_SIZE = 1000;
    private static final int TIMEOUT_MS = 60000; // 60 seconds

    private final Map<String,FulgoraRelationQuery> queryDefinitions;
    private final int numQueries;
    private final List<BlockingQueue<QueryResult>> dataQueues;
    private final DataPuller[] pullThreads;
    private final int numProcessors;
    private final StandardTitanTx tx;
    private final EdgeSerializer edgeSerializer;
    private final IDManager idManager;
    private final OLAPJob<S> job;
    private final ConcurrentMap<Long,MessageAccumulator> partitionedVertexMsgs;

    final FulgoraResult<S> vertexStates;
    final String stateKey;
    final StateInitializer<S> initializer;

    private boolean processingException = false;

    FulgoraExecutor(final Map<String,FulgoraRelationQuery> queryDefs, final StandardTitanTx tx, final IDManager idManager,
                    final int numProcessors, final String stateKey, final OLAPJob job,
                    final StateInitializer<S> initializer, final FulgoraResult<S> initialState) {
        this.tx=tx;
        this.edgeSerializer = tx.getEdgeSerializer();
        this.stateKey = stateKey;
        this.job = job;
        this.initializer = initializer;
        BackendTransaction btx = tx.getTxHandle();
        this.idManager = idManager;
        this.queryDefinitions = queryDefs;
        this.numProcessors = numProcessors;

        int count = 0;
        for (FulgoraRelationQuery frq : queryDefs.values()) count+=frq.queries.size();
        numQueries = count+1;
        //Add one for grounding query, since the first (0th) query is the grounding/condition query
        dataQueues = new ArrayList<BlockingQueue<QueryResult>>(numQueries);
        pullThreads = new DataPuller[numQueries];

        int pos = 0;
        pullThreads[pos++]=addDataPuller(BaseKey.VertexExists.getName(),
                new SliceQuery(BufferUtil.zeroBuffer(4),BufferUtil.oneBuffer(4)).setLimit(1),btx);
        for (Map.Entry<String,FulgoraRelationQuery> queryDef : queryDefs.entrySet()) {
            String queryName = queryDef.getKey();
            List<SliceQuery> sqs = queryDef.getValue().queries;
            for (SliceQuery sq : sqs) {
                pullThreads[pos++]=addDataPuller(queryName,sq,btx);
            }
        }

        partitionedVertexMsgs = new ConcurrentHashMap<Long, MessageAccumulator>();
        vertexStates = initialState;
    }

    StandardTitanTx tx() {
        return tx;
    }

    private final DataPuller addDataPuller(String queryName, SliceQuery sq, BackendTransaction btx) {
        BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<QueryResult>(QUEUE_SIZE);
        dataQueues.add(queue);
        DataPuller dp = new DataPuller(idManager, queryName, queue, btx.edgeStoreKeys(sq));
        dp.start();
        return dp;
    }

    S getVertexState(long vertexId) {
        if (IDManager.VertexIDType.Hidden.is(vertexId)) return null;
        if (idManager.isPartitionedVertex(vertexId)) vertexId=idManager.getCanonicalVertexId(vertexId);
        S state = vertexStates.get(vertexId);
        if (state==null) {
            state = initializer.initialState();
//            setVertexState(vertexId, state);
        }
        return state;
    }

    void setVertexState(long vertexId, S state) {
        vertexStates.set(vertexId, state);
    }

    @Override
    public void run() {
        ThreadPoolExecutor processor = new ThreadPoolExecutor(numProcessors, numProcessors, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(QUEUE_SIZE));
        processor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            QueryResult[] currentResults = new QueryResult[numQueries];
            while (true) {
                for (int i = 0; i < numQueries; i++) {
                    if (currentResults[i]!=null) continue;
                    BlockingQueue<QueryResult> queue = dataQueues.get(i);

                    QueryResult qr = queue.poll(10,TimeUnit.MILLISECONDS); //Try very short time to see if we are done
                    if (qr==null) {
                        if (pullThreads[i].isFinished()) continue; //No more data to be expected
                        qr = queue.poll(TIMEOUT_MS,TimeUnit.MILLISECONDS); //otherwise, give it more time
                        if (qr==null && !pullThreads[i].isFinished())
                            throw new TitanException("Timed out waiting for next vertex data - storage error likely");
                    }
                    currentResults[i]=qr;
                }
                QueryResult conditionQuery = currentResults[0];
                currentResults[0]=null;
                if (conditionQuery==null) break; //Termination condition - primary query has no more data

                //First, check if this is a valid (non-deleted) vertex
                RelationCache relCache = tx.getEdgeSerializer().parseRelation(
                                        conditionQuery.entries.get(0),true,tx);
                final long vertexid = conditionQuery.vertexId;
                if (relCache.typeId != BaseKey.VertexExists.getID() &&
                        (!idManager.isPartitionedVertex(vertexid) || idManager.isCanonicalVertexId(vertexid)) ) {
                    log.warn("Found deleted vertex with id: {}|{}|{}. Skipping",conditionQuery.vertexId,idManager.isPartitionedVertex(vertexid),relCache);
                    if (idManager.isPartitionedVertex(vertexid)) getMessageAccumulator(vertexid).markDeleted();
                    for (int i=1;i<currentResults.length;i++) {
                        if (currentResults[i]!=null && currentResults[i].vertexId==conditionQuery.vertexId) {
                            currentResults[i]=null;
                        }
                    }
                } else {
                    List<QueryResult> queryResults = new ArrayList<QueryResult>(numQueries-1);
                    for (int i=1;i<currentResults.length;i++) {
                        if (currentResults[i]!=null && currentResults[i].vertexId==vertexid) {
                            queryResults.add(currentResults[i]);
//                            vertex.addToQueryCache(queries[i],currentResults[i].entries);
                            currentResults[i]=null;
                        }
                    }
                    processor.submit(new VertexProcessor(vertexid, queryResults));

                }
            }
            processor.shutdown();
            processor.awaitTermination(TIMEOUT_MS,TimeUnit.MILLISECONDS);
            if (!processor.isTerminated()) throw new TitanException("Timed out waiting for vertex processors");
            for (int i = 0; i < pullThreads.length; i++) {
                pullThreads[i].join(10);
                if (pullThreads[i].isAlive()) throw new TitanException("Could not join data pulling thread");
            }

            //Process partitioned vertices since we have now accumulated all their state
            if (!partitionedVertexMsgs.isEmpty()) {
                processor = new ThreadPoolExecutor(numProcessors, numProcessors, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(QUEUE_SIZE));
                processor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
                for (final Map.Entry<Long,MessageAccumulator> partitionVertexMsg : partitionedVertexMsgs.entrySet()) {
                    final MessageAccumulator mergedMsg = partitionVertexMsg.getValue();
                    if (mergedMsg.isDeleted()) {
                        log.warn("Found deleted partitioned vertex with id: {}. Skipping",partitionVertexMsg.getKey());
                        continue;
                    }
                    final FulgoraVertex vertex = new FulgoraVertex(tx,partitionVertexMsg.getKey(),FulgoraExecutor.this);
                    processor.submit(new Runnable() {
                        @Override
                        public void run() {
                            processVertex(vertex,mergedMsg);
                        }
                    });
                }
                processor.shutdown();
                processor.awaitTermination(TIMEOUT_MS,TimeUnit.MILLISECONDS);
                if (!processor.isTerminated()) throw new TitanException("Timed out waiting for partitioned-vertex processors");
            }

            tx.rollback();
            set(vertexStates);
        } catch (Throwable e) {
            log.error("Exception occured during job execution: {}",e);
            setException(e);
        } finally {
            processor.shutdownNow();
        }
    }

//    final QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery> edgeProcessor = new QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery>() {
//
//        @Override
//        public Iterator<TitanRelation> getNew(VertexCentricQuery query) {
//            return Iterators.emptyIterator();
//        }
//
//        @Override
//        public boolean hasDeletions(VertexCentricQuery query) {
//            return false;
//        }
//
//        @Override
//        public boolean isDeleted(VertexCentricQuery query, TitanRelation result) {
//            return false;
//        }
//
//        @Override
//        public Iterator<TitanRelation> execute(VertexCentricQuery query, SliceQuery sq, Object exeInfo) {
//            assert exeInfo==null;
//
//            final InternalVertex vertex = query.getVertex();
//
//            Iterable<Entry> iter = vertex.loadRelations(sq, new Retriever<SliceQuery, EntryList>() {
//                @Override
//                public EntryList get(SliceQuery query) {
//                    return StaticArrayEntryList.EMPTY_LIST;
//                }
//            });
//
//            return Iterables.transform(iter, new Function<Entry, TitanRelation>() {
//                @Override
//                public TitanRelation apply(@Nullable Entry data) {
//                    return RelationConstructor.readRelation(vertex,data,edgeSerializer,tx,neighborVertices);
//                }
//            }).iterator();
//        }
//    };

    private final VertexFactory neighborVertices = new VertexFactory() {
        @Override
        public InternalVertex getInternalVertex(long id) {
            return new FulgoraNeighborVertex(id,FulgoraExecutor.this);
        }
    };


    private class VertexProcessor implements Runnable {

        final long vertexId;
        final List<QueryResult> queryResults;

        private VertexProcessor(long vertexId, List<QueryResult> queryResults) {
            this.vertexId = vertexId;
            this.queryResults = queryResults;
        }

        @Override
        public void run() {
            try {
                FulgoraVertex vertex = new FulgoraVertex(tx,vertexId,FulgoraExecutor.this);
                Map<String,Object> pulledMessages = new HashMap<String,Object>(queryDefinitions.size());
                for (QueryResult qr : queryResults) {
                    String queryName = qr.queryName;
                    FulgoraRelationQuery frq = queryDefinitions.get(queryName);
                    Preconditions.checkState(frq!=null && qr.vertexId==vertexId);
                    Object combinedMsg = pulledMessages.get(queryName);
                    Iterator<Entry> iter = qr.entries.reuseIterator();
                    while (iter.hasNext()) {
                        Entry data = iter.next();
                        TitanRelation r = RelationConstructor.readRelation(vertex,data,edgeSerializer,tx,neighborVertices);
                        if (frq instanceof FulgoraPropertyQuery) {
                            Preconditions.checkArgument(r instanceof TitanProperty);
                            combinedMsg = ((FulgoraPropertyQuery)frq).process((TitanProperty)r,combinedMsg);
                        } else {
                            assert frq instanceof FulgoraEdgeQuery;
                            Preconditions.checkArgument(r instanceof TitanEdge);
                            combinedMsg = ((FulgoraEdgeQuery)frq).process((TitanEdge)r, edgeSerializer.parseDirection(data),
                                    getVertexState(((TitanEdge) r).getOtherVertex(vertex).getID()),combinedMsg);
                        }
                    }
                    if (combinedMsg!=null) pulledMessages.put(queryName,combinedMsg);
                }
                if (idManager.isPartitionedVertex(vertexId)) {
                    getMessageAccumulator(vertexId).add(pulledMessages);
                } else {
                    //Process entire vertex directly
                    processVertex(vertex,pulledMessages);
                }
            } catch (Throwable e) {
                log.error("Exception processing relations for ["+vertexId+"]: ",e);
                processingException = true;
            }

        }
    }

    private MessageAccumulator getMessageAccumulator(long partitionedVertexId) {
        Preconditions.checkArgument(idManager.isPartitionedVertex(partitionedVertexId));
        long canonicalId = idManager.getCanonicalVertexId(partitionedVertexId);
        //Only accumulate messages for later processing
        MessageAccumulator accMsg = partitionedVertexMsgs.get(canonicalId);
        if (accMsg==null) {
            partitionedVertexMsgs.putIfAbsent(canonicalId, new MessageAccumulator());
            accMsg = partitionedVertexMsgs.get(canonicalId);
        }
        return accMsg;
    }

    private class MessageAccumulator extends HashMap<String,Object> {

        private boolean isDeleted = false;

        private MessageAccumulator() {
            super(queryDefinitions.size());
        }

        private synchronized void add(Map<String,Object> pulledMessages) {
            for (Map.Entry<String,Object> add : pulledMessages.entrySet()) {
                if (add.getValue()==null) continue;
                String key = add.getKey();
                Object existing = super.get(key);
                if (existing==null) super.put(key,add.getValue());
                else super.put(key,queryDefinitions.get(key).combiner.combine(existing,add.getValue()));
            }
        }

        private void markDeleted() {
            this.isDeleted=true;
        }

        private boolean isDeleted() {
            return isDeleted;
        }

    }


    private void processVertex(FulgoraVertex<S> vertex, Map<String,Object> pulledMessages) {
        long vertexId = vertex.getID();
        try {
            vertex.setProcessedProperties(pulledMessages);
            S newState = job.process(vertex);
            setVertexState(vertexId,newState);
        } catch (Throwable e) {
            log.error("Exception processing vertex ["+vertexId+"]: ",e);
            processingException = true;
            setVertexState(vertexId,null); //Invalidate state
        }
    }


    private static class DataPuller extends Thread {

        private final BlockingQueue<QueryResult> queue;
        private final String queryName;
        private final KeyIterator keyIter;
        private final IDManager idManager;
        private volatile boolean finished;

        private DataPuller(IDManager idManager, String queryName,
                           BlockingQueue<QueryResult> queue, KeyIterator keyIter) {
            this.queryName = queryName;
            this.queue = queue;
            this.keyIter = keyIter;
            this.idManager = idManager;
            this.finished = false;
        }

        @Override
        public void run() {
            try {
                while (keyIter.hasNext()) {
                    StaticBuffer key = keyIter.next();
                    RecordIterator<Entry> entries = keyIter.getEntries();
                    long vertexId = idManager.getKeyID(key);
                    if (IDManager.VertexIDType.Hidden.is(vertexId)) continue;
                    EntryList entryList = StaticArrayEntryList.ofStaticBuffer(entries, StaticArrayEntry.ENTRY_GETTER);
                    try {
                        queue.put(new QueryResult(queryName,vertexId,entryList));
                    } catch (InterruptedException e) {
                        log.error("Data-pulling thread interrupted while waiting on queue",e);
                        break;
                    }
                }
                finished = true;
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

        public boolean isFinished() {
            return finished;
        }
    }

    private static class QueryResult {

        final EntryList entries;
        final long vertexId;
        final String queryName;

        private QueryResult(String queryName, long vertexId, EntryList entries) {
            this.entries = entries;
            this.vertexId = vertexId;
            this.queryName = queryName;
        }
    }

}
