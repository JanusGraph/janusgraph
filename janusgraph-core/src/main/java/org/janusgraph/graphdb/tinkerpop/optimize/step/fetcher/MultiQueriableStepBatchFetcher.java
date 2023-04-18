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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Common logic for {@link  org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable  MultiQueriable} steps
 * to prefetch data for multiple vertices using multiQuery.
 */
public abstract class MultiQueriableStepBatchFetcher<R> {

    private final Set<JanusGraphVertex> verticesToPrefetch = new HashSet<>();

    private Map<JanusGraphVertex, R> multiQueryResults = null;

    public void registerFutureVertexForPrefetching(Vertex futureVertex) {
        verticesToPrefetch.add((JanusGraphVertex) futureVertex);
    }

    public R fetchData(final Traversal.Admin<?, ?> traversal, Vertex forVertex){
        if (hasNoFetchedData(forVertex)) {
            prefetchNextBatch(traversal, forVertex);
        }
        return multiQueryResults.get(forVertex);
    }

    protected boolean hasNoFetchedData(Vertex forVertex){
        return multiQueryResults == null || !multiQueryResults.containsKey(forVertex);
    }

    public void prefetchNextBatch(final Traversal.Admin<?, ?> traversal, Vertex requiredFetchVertex){

        verticesToPrefetch.add((JanusGraphVertex) requiredFetchVertex);
        final JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery(verticesToPrefetch);
        verticesToPrefetch.clear();

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

    protected abstract Map<JanusGraphVertex, R> makeQueryAndExecute(JanusGraphMultiVertexQuery multiQuery);

}
