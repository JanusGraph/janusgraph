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

    private Map<JanusGraphVertex, R> multiQueryResults = null;
    private int batchSize;
    private int currentLoops = 0;
    private BatchProcessingQueue<JanusGraphVertex> firstLoopBatchProcessingQueue;
    private BatchProcessingQueue<JanusGraphVertex> currentLoopBatchProcessingQueue;
    private BatchProcessingQueue<JanusGraphVertex> nextLoopBatchProcessingQueue;

    public MultiQueriableStepBatchFetcher(int batchSize){
        this.batchSize = batchSize;
        this.currentLoopBatchProcessingQueue = generateNewBatchProcessingQueue();
        this.nextLoopBatchProcessingQueue = generateNewBatchProcessingQueue();
    }

    public void registerCurrentLoopFutureVertexForPrefetching(Vertex forGeneralVertex, int traverserLoops) {
        ensureCorrectLoopQueues(traverserLoops);
        JanusGraphVertex forVertex = JanusGraphTraversalUtil.getJanusGraphVertex(forGeneralVertex);
        if(traverserLoops != 0 || firstLoopBatchProcessingQueue == null || !firstLoopBatchProcessingQueue.hasElementInAnyBatch(forVertex)){
            currentLoopBatchProcessingQueue.addToBatchToEnd(forVertex);
        } else {
            // If the above `if` check fails it means that the next time `fetchData` is called - it should be the first iteration of the
            // next loop. Thus, it means that `firstLoopBatchProcessingQueue` will contain all the necessary vertices for
            // the loop (as well as potential vertices for the next loops). Thus, we won't need to add anything to `currentLoopBatchProcessingQueue`
            // until `traverserLoops == 0`.
            // Moreover, we remove the element from `currentLoopBatchProcessingQueue` to ensure that the next time `fetchData` is called
            // we guaranteed to switch to replace `currentLoopBatchProcessingQueue` with `firstLoopBatchProcessingQueue`.
            currentLoopBatchProcessingQueue.softRemoveFromAllElementsRegistration(forVertex);
        }
    }

    public void registerNextLoopFutureVertexForPrefetching(Vertex forGeneralVertex, int traverserLoops) {
        ensureCorrectLoopQueues(traverserLoops);
        nextLoopBatchProcessingQueue.addToBatchToEnd(JanusGraphTraversalUtil.getJanusGraphVertex(forGeneralVertex));
    }

    public void registerFirstNewLoopFutureVertexForPrefetching(Vertex forGeneralVertex) {
        if(firstLoopBatchProcessingQueue == null){
            firstLoopBatchProcessingQueue = generateNewBatchProcessingQueue();
        }
        firstLoopBatchProcessingQueue.addToBatchToEnd(JanusGraphTraversalUtil.getJanusGraphVertex(forGeneralVertex));
    }

    public void refreshIfLoopsAreReset(int traverserLoops, JanusGraphVertex forVertex){
        if(traverserLoops != 0 || currentLoopBatchProcessingQueue.hasElementInAnyBatch(forVertex)){
            return;
        }
        currentLoops = traverserLoops;
        nextLoopBatchProcessingQueue = generateNewBatchProcessingQueue();
        currentLoopBatchProcessingQueue = firstLoopBatchProcessingQueue == null ? generateNewBatchProcessingQueue() : firstLoopBatchProcessingQueue;
        firstLoopBatchProcessingQueue = null;
    }

    public void setBatchSize(int batchSize){
        this.batchSize = batchSize;
        this.currentLoopBatchProcessingQueue.setBatchSize(batchSize);
        this.nextLoopBatchProcessingQueue.setBatchSize(batchSize);
    }

    public R fetchData(final Traversal.Admin<?, ?> traversal, Vertex forGeneralVertex, int traverserLoops){
        JanusGraphVertex forVertex = JanusGraphTraversalUtil.getJanusGraphVertex(forGeneralVertex);
        if (hasNoFetchedData(forVertex)) {
            refreshIfLoopsAreReset(traverserLoops, forVertex);
            ensureCorrectLoopQueues(traverserLoops);
            prefetchNextBatch(traversal, forVertex);
        } else {
            ensureCorrectLoopQueues(traverserLoops);
        }
        return multiQueryResults.get(forVertex);
    }

    private boolean hasNoFetchedData(Vertex forVertex){
        return multiQueryResults == null || !multiQueryResults.containsKey(forVertex);
    }

    private void prefetchNextBatch(final Traversal.Admin<?, ?> traversal, JanusGraphVertex requiredFetchVertex){
        final JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal)
            .multiQuery(currentLoopBatchProcessingQueue.removeFirst());
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

    protected abstract Map<JanusGraphVertex, R> makeQueryAndExecute(JanusGraphMultiVertexQuery multiQuery);

}
