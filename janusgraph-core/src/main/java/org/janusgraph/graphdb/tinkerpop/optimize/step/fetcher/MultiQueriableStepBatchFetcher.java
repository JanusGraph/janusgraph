// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;

import java.util.Map;

/**
 * Common logic for {@link  org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable  MultiQueriable} steps
 * to prefetch data for multiple vertices using multiQuery.
 */
public abstract class MultiQueriableStepBatchFetcher<R> {

    //private final Map<Integer, BatchProcessingQueue<JanusGraphVertex>> verticesToPrefetchByLoop = new HashMap<>();

    private Map<JanusGraphVertex, R> multiQueryResults = null;
    private int batchSize;
    private int currentLoops = 0;
    private BatchProcessingQueue<JanusGraphVertex> currentLoopBatchProcessingQueue;
    private BatchProcessingQueue<JanusGraphVertex> nextLoopBatchProcessingQueue;

    public MultiQueriableStepBatchFetcher(int batchSize){
        this.batchSize = batchSize;
        this.currentLoopBatchProcessingQueue = generateNewBatchProcessingQueue();
        this.nextLoopBatchProcessingQueue = generateNewBatchProcessingQueue();
    }

    public void registerCurrentLoopFutureVertexForPrefetching(Vertex futureVertexTraverser, int traverserLoops) {
        ensureCorrectLoopQueues(traverserLoops);
        currentLoopBatchProcessingQueue.addToBatch(JanusGraphTraversalUtil.getJanusGraphVertex(futureVertexTraverser));
    }

    public void registerNextLoopFutureVertexForPrefetching(Vertex futureVertexTraverser, int traverserLoops) {
        ensureCorrectLoopQueues(traverserLoops);
        nextLoopBatchProcessingQueue.addToBatch(JanusGraphTraversalUtil.getJanusGraphVertex(futureVertexTraverser));
    }

    public R fetchData(final Traversal.Admin<?, ?> traversal, Vertex forVertexTraverser, int traverserLoops){
        ensureCorrectLoopQueues(traverserLoops);
        JanusGraphVertex forVertex = JanusGraphTraversalUtil.getJanusGraphVertex(forVertexTraverser);
        if (hasNoFetchedData(forVertex)) {
            prefetchNextBatch(traversal, forVertex);
        }
        return multiQueryResults.get(forVertex);
    }

    protected boolean hasNoFetchedData(Vertex forVertex){
        return multiQueryResults == null || !multiQueryResults.containsKey(forVertex);
    }

    public void prefetchNextBatch(final Traversal.Admin<?, ?> traversal, JanusGraphVertex requiredFetchVertex){
        final JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal)
            .multiQuery(currentLoopBatchProcessingQueue.pollBatch());
        multiQuery.addVertex(requiredFetchVertex);
        try {
            multiQueryResults = makeQueryAndExecute(multiQuery);
        } catch (JanusGraphException janusGraphException) {
            if (janusGraphException.isCausedBy(InterruptedException.class)) {
                TraversalInterruptedException traversalInterruptedException = new TraversalInterruptedException();
                traversalInterruptedException.initCause(janusGraphException);
                throw traversalInterruptedException;
            }
            throw janusGraphException;
        }
    }

    private void ensureCorrectLoopQueues(int loops){
        if(loops != currentLoops){
            currentLoopBatchProcessingQueue = loops == currentLoops + 1 ?
                nextLoopBatchProcessingQueue : generateNewBatchProcessingQueue();
            nextLoopBatchProcessingQueue = generateNewBatchProcessingQueue();
            currentLoops = loops;
        }
    }

    private BatchProcessingQueue<JanusGraphVertex> generateNewBatchProcessingQueue(){
        return new BatchProcessingQueue<>(batchSize);
    }

    public void setBatchSize(int batchSize){
        this.batchSize = batchSize;
        this.currentLoopBatchProcessingQueue.setBatchSize(batchSize);
        this.nextLoopBatchProcessingQueue.setBatchSize(batchSize);
    }

    protected abstract Map<JanusGraphVertex, R> makeQueryAndExecute(JanusGraphMultiVertexQuery multiQuery);

}
