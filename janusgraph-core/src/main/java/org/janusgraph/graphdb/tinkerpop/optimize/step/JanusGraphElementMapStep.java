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

package org.janusgraph.graphdb.tinkerpop.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher.LabelStepBatchFetcher;
import org.janusgraph.graphdb.tinkerpop.optimize.step.service.DirectPropertiesFetchingService;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.janusgraph.graphdb.util.CopyStepUtil;
import org.janusgraph.graphdb.util.JanusGraphTraverserUtil;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class JanusGraphElementMapStep <K, E> extends ElementMapStep<K, E> implements Profiling, MultiQueriable<Element,Map<K, E>> {

    private boolean useMultiQuery = false;
    private LabelStepBatchFetcher labelStepBatchFetcher;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private int batchSize = Integer.MAX_VALUE;
    private final boolean prefetchAllPropertiesRequired;
    private final boolean prefetchingAllowed;
    private DirectPropertiesFetchingService directPropertiesFetchingService;

    public JanusGraphElementMapStep(ElementMapStep<K, E> originalStep, boolean prefetchAllPropertiesRequired, boolean prefetchingAllowed) {
        super(originalStep.getTraversal(), originalStep.getPropertyKeys());
        CopyStepUtil.copyAbstractStepModifiableFields(originalStep, this);
        this.prefetchAllPropertiesRequired = prefetchAllPropertiesRequired;
        this.prefetchingAllowed = prefetchingAllowed;
        if(originalStep.isOnGraphComputer()){
            onGraphComputer();
        }
        if (originalStep instanceof JanusGraphElementMapStep) {
            JanusGraphElementMapStep originalJanusGraphElementMapStep = (JanusGraphElementMapStep) originalStep;
            setBatchSize(originalJanusGraphElementMapStep.batchSize);
            setUseMultiQuery(originalJanusGraphElementMapStep.useMultiQuery);
        }
    }

    @Override
    protected Map<K, E> map(final Traverser.Admin<Element> traverser) {
        if (useMultiQuery && traverser.get() instanceof Vertex) {
            Map<Object, Object> map = new LinkedHashMap();
            Vertex vertexToFetch = (Vertex) traverser.get();
            int loops = JanusGraphTraverserUtil.getLoops(traverser);
            addElementProperties(map, vertexToFetch, loops);
            addIncludedOptions(map, vertexToFetch, loops);
            return (Map) map;
        }
        return super.map(traverser);
    }

    private void addElementProperties(Map<Object, Object> map, Vertex vertexToFetch, int loops){
        Iterator<? extends Property> properties = directPropertiesFetchingService
            .fetchProperties(getTraversal(), vertexToFetch, loops);
        while (properties.hasNext()) {
            final Property<?> property = properties.next();
            map.put(property.key(), property.value());
        }
    }

    private void addIncludedOptions(Map<Object, Object> map, Vertex vertexToFetch, int loops){
        map.put(T.id, vertexToFetch.id());
        map.put(T.label, labelStepBatchFetcher.fetchData(getTraversal(), vertexToFetch, loops));
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
        if(directPropertiesFetchingService != null){
            directPropertiesFetchingService.setQueryProfiler(queryProfiler);
        }
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = prefetchingAllowed && useMultiQuery;
        if(this.useMultiQuery){
            if(directPropertiesFetchingService == null){
                directPropertiesFetchingService = new DirectPropertiesFetchingService(getPropertyKeys(),
                    batchSize, prefetchAllPropertiesRequired, queryProfiler);
            }
            if(labelStepBatchFetcher == null){
                labelStepBatchFetcher = new LabelStepBatchFetcher(JanusGraphElementMapStep.this::makeLabelsQuery, batchSize);
            }
        }
    }

    private <Q extends BaseVertexQuery> Q makeLabelsQuery(Q query) {
        return (Q) BasicVertexCentricQueryUtil.withLabelVertices((BasicVertexCentricQueryBuilder) query)
            .profiler(queryProfiler);
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        if(directPropertiesFetchingService != null){
            directPropertiesFetchingService.setBatchSize(batchSize);
        }
        if(labelStepBatchFetcher != null){
            labelStepBatchFetcher.setBatchSize(batchSize);
        }
    }

    @Override
    public void registerFirstNewLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            labelStepBatchFetcher.registerFirstNewLoopFutureVertexForPrefetching(futureVertex);
            directPropertiesFetchingService.registerFirstNewLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }

    @Override
    public void registerSameLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            labelStepBatchFetcher.registerCurrentLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
            directPropertiesFetchingService.registerSameLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }

    @Override
    public void registerNextLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            labelStepBatchFetcher.registerNextLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
            directPropertiesFetchingService.registerNextLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }
}
