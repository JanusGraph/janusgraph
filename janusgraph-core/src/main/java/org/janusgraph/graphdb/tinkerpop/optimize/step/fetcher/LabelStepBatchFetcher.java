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

import com.google.common.collect.Iterables;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryUtil;

import java.util.HashMap;
import java.util.Map;

public class LabelStepBatchFetcher extends MultiQueriableStepBatchFetcher<String>{

    private final FetchQueryBuildFunction fetchQueryBuildFunction;

    public LabelStepBatchFetcher(FetchQueryBuildFunction fetchQueryBuildFunction, int batchSize) {
        super(batchSize);
        this.fetchQueryBuildFunction = fetchQueryBuildFunction;
    }

    @Override
    protected Map<JanusGraphVertex, String> makeQueryAndExecute(JanusGraphMultiVertexQuery multiQuery) {
        multiQuery = fetchQueryBuildFunction.makeQuery(multiQuery);
        Map<JanusGraphVertex, Iterable<JanusGraphVertex>> labelsBatch = multiQuery.vertices();
        Map<JanusGraphVertex, String> result = new HashMap<>(labelsBatch.size());
        for(Map.Entry<JanusGraphVertex, Iterable<JanusGraphVertex>> labelEntry : labelsBatch.entrySet()){
            result.put(
                labelEntry.getKey(),
                BasicVertexCentricQueryUtil.castToVertexLabel(Iterables.getOnlyElement(labelEntry.getValue(),null)).name()
            );
        }
        return result;
    }

}
