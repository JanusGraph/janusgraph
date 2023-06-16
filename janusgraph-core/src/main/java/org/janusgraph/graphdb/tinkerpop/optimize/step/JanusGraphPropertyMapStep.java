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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher.LabelStepBatchFetcher;
import org.janusgraph.graphdb.tinkerpop.optimize.step.service.DirectPropertiesFetchingService;
import org.janusgraph.graphdb.tinkerpop.optimize.step.service.PropertiesFetchingService;
import org.janusgraph.graphdb.tinkerpop.optimize.step.service.TraversalPropertiesFetchingService;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.janusgraph.graphdb.util.CopyStepUtil;
import org.janusgraph.graphdb.util.JanusGraphTraverserUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JanusGraphPropertyMapStep<K, E> extends PropertyMapStep<K, E> implements Profiling, MultiQueriable<Element,Map<K, E>> {

    private boolean useMultiQuery = false;
    private LabelStepBatchFetcher labelStepBatchFetcher;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private int batchSize = Integer.MAX_VALUE;
    private boolean withIdsFetching;
    private boolean withLabelsFetching;
    private final boolean prefetchAllPropertiesRequired;
    private final boolean prefetchingAllowed;
    private PropertiesFetchingService propertiesFetchingService;
    private final boolean isReturnTypeValue;

    public JanusGraphPropertyMapStep(PropertyMapStep<K, E> originalStep, boolean prefetchAllPropertiesRequired, boolean prefetchingAllowed) {
        super(originalStep.getTraversal(), originalStep.getReturnType(), originalStep.getTraversalRing(), originalStep.getPropertyKeys());
        CopyStepUtil.copyAbstractStepModifiableFields(originalStep, this);
        this.prefetchAllPropertiesRequired = prefetchAllPropertiesRequired;
        this.prefetchingAllowed = prefetchingAllowed;
        this.isReturnTypeValue = getReturnType() == PropertyType.VALUE;

        tokens = originalStep.getIncludedTokens();
        withIdsFetching = includeToken(WithOptions.ids);
        withLabelsFetching = includeToken(WithOptions.labels);
        traversalRing.getTraversals().forEach(this::integrateChild);
        parameters = originalStep.getParameters();
        parameters.getTraversals().forEach(this::integrateChild);

        Traversal.Admin<Element, ? extends Property> propertyTraversal = originalStep.getPropertyTraversal();
        if(propertyTraversal != null){
            super.setPropertyTraversal(propertyTraversal);
        }

        if (originalStep instanceof JanusGraphPropertyMapStep) {
            JanusGraphPropertyMapStep originalJanusGraphPropertyMapStep = (JanusGraphPropertyMapStep) originalStep;
            setBatchSize(originalJanusGraphPropertyMapStep.batchSize);
            setUseMultiQuery(originalJanusGraphPropertyMapStep.useMultiQuery);
        }
    }

    @Override
    public void configure(final Object... keyValues) {
        super.configure(keyValues);
        withIdsFetching = includeToken(WithOptions.ids);
        withLabelsFetching = includeToken(WithOptions.labels);
        createLabelFetcherIfNeeded();
    }

    @Override
    public void setPropertyTraversal(final Traversal.Admin<Element, ? extends Property> propertyTraversal) {
        super.setPropertyTraversal(propertyTraversal);
        propertiesFetchingService = null;
        setUseMultiQuery(useMultiQuery);
    }

    @Override
    protected Map<K, E> map(final Traverser.Admin<Element> traverser) {
        if (useMultiQuery && traverser.get() instanceof Vertex) {
            Map<Object, Object> map = new LinkedHashMap();
            addElementPropertiesInternal(traverser, map);
            addIncludedOptions(map, traverser);
            applyTraversalRingToMap(map);
            return (Map) map;
        } else {
            return super.map(traverser);
        }
    }

    private void addElementPropertiesInternal(final Traverser.Admin<Element> traverser, Map<Object, Object> map) {
        Iterator<? extends Property> properties = propertiesFetchingService.fetchProperties(traverser, traversal);
        while(properties.hasNext()) {
            Property<?> property = (Property)properties.next();
            Object value = isReturnTypeValue ? property.value() : property;
            map.compute(property.key(), (k, v) -> {
                List<Object> values = v != null ? (List)v : new ArrayList<>();
                values.add(value);
                return values;
            });
        }
    }

    private void addIncludedOptions(Map<Object, Object> map, final Traverser.Admin<Element> traverser){
        if (this.returnType == PropertyType.VALUE) {
            Vertex vertexToFetch = (Vertex) traverser.get();
            if (withIdsFetching) {
                map.put(T.id, getElementId(vertexToFetch));
            }
            if (withLabelsFetching) {
                map.put(T.label, labelStepBatchFetcher.fetchData(getTraversal(), vertexToFetch, JanusGraphTraverserUtil.getLoops(traverser)));
            }
        }
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
        if(propertiesFetchingService instanceof DirectPropertiesFetchingService){
            ((DirectPropertiesFetchingService) propertiesFetchingService).setQueryProfiler(queryProfiler);
        } else if(propertiesFetchingService != null) {
            propertiesFetchingService.setMetrics(metrics);
        }
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = prefetchingAllowed && useMultiQuery;
        if(this.useMultiQuery && propertiesFetchingService == null){
            final Traversal.Admin<Element, ? extends Property> propertyTraversal = getPropertyTraversal();
            propertiesFetchingService = propertyTraversal == null ?
                new DirectPropertiesFetchingService(getPropertyKeys(), batchSize, prefetchAllPropertiesRequired, queryProfiler) :
                new TraversalPropertiesFetchingService(propertyTraversal, batchSize, prefetchAllPropertiesRequired);
        }
        createLabelFetcherIfNeeded();
    }

    private void createLabelFetcherIfNeeded(){
        if(withLabelsFetching && useMultiQuery && labelStepBatchFetcher == null){
            labelStepBatchFetcher = new LabelStepBatchFetcher(JanusGraphPropertyMapStep.this::makeLabelsQuery, batchSize);
        }
    }

    private <Q extends BaseVertexQuery> Q makeLabelsQuery(Q query) {
        return (Q) BasicVertexCentricQueryUtil.withLabelVertices((BasicVertexCentricQueryBuilder) query)
            .profiler(queryProfiler);
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        if(propertiesFetchingService != null){
            propertiesFetchingService.setBatchSize(batchSize);
        }
        if(labelStepBatchFetcher != null){
            labelStepBatchFetcher.setBatchSize(batchSize);
        }
    }

    @Override
    public void registerFirstNewLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            propertiesFetchingService.registerFirstNewLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
            if(withLabelsFetching){
                labelStepBatchFetcher.registerFirstNewLoopFutureVertexForPrefetching(futureVertex);
            }
        }
    }

    @Override
    public void registerSameLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            propertiesFetchingService.registerSameLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
            if(withLabelsFetching) {
                labelStepBatchFetcher.registerCurrentLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
            }
        }
    }

    @Override
    public void registerNextLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            propertiesFetchingService.registerNextLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
            if(withLabelsFetching) {
                labelStepBatchFetcher.registerNextLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
            }
        }
    }
}
