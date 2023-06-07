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

package org.janusgraph.graphdb.tinkerpop.optimize.step.service;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher.PropertiesStepBatchFetcher;
import org.janusgraph.graphdb.tinkerpop.optimize.step.util.PropertiesFetchingUtil;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.janusgraph.graphdb.util.JanusGraphTraverserUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirectPropertiesFetchingService implements PropertiesFetchingService {

    private final PropertiesStepBatchFetcher propertiesStepBatchFetcher;
    private final String[] propertyKeys;
    private final Set<String> propertyKeysSet;
    private final boolean prefetchAllPropertiesRequired;
    private QueryProfiler queryProfiler;

    public DirectPropertiesFetchingService(String[] propertyKeys, int batchSize, boolean prefetchAllPropertiesRequired,
                                           QueryProfiler queryProfiler) {
        this.propertyKeys = propertyKeys;
        this.propertyKeysSet = new HashSet<>(Arrays.asList(propertyKeys));
        this.prefetchAllPropertiesRequired = prefetchAllPropertiesRequired;
        this.queryProfiler = queryProfiler;
        this.propertiesStepBatchFetcher = new PropertiesStepBatchFetcher(this::makePropertiesQuery, batchSize);
    }

    @Override
    public Iterator<? extends Property> fetchProperties(Traverser.Admin<Element> traverser, Traversal.Admin<?, ?> traversal) {
        return fetchProperties(traversal, (Vertex) traverser.get(), JanusGraphTraverserUtil.getLoops(traverser));
    }

    public Iterator<? extends Property> fetchProperties(Traversal.Admin<?, ?> traversal, Vertex vertex, int loops) {
        return PropertiesFetchingUtil
            .filterPropertiesIfNeeded(
                propertiesStepBatchFetcher.fetchData(traversal, vertex, loops).iterator(),
                prefetchAllPropertiesRequired,
                propertyKeysSet);
    }

    public <Q extends BaseVertexQuery> Q makePropertiesQuery(Q query) {
        return PropertiesFetchingUtil.makeBasePropertiesQuery(query, prefetchAllPropertiesRequired, propertyKeysSet, propertyKeys, queryProfiler);
    }

    @Override
    public void registerFirstNewLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        propertiesStepBatchFetcher.registerFirstNewLoopFutureVertexForPrefetching(futureVertex);
    }

    @Override
    public void registerSameLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        propertiesStepBatchFetcher.registerCurrentLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
    }

    @Override
    public void registerNextLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        propertiesStepBatchFetcher.registerNextLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }

    public void setQueryProfiler(QueryProfiler queryProfiler){
        this.queryProfiler = queryProfiler;
    }

    @Override
    public void setBatchSize(int batchSize){
        propertiesStepBatchFetcher.setBatchSize(batchSize);
    }
}
