// Copyright 2017 JanusGraph Authors
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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher.VertexStepBatchFetcher;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphVertexStep<E extends Element> extends VertexStep<E> implements HasStepFolder<Vertex, E>, Profiling, MultiQueriable<Vertex,E> {

    private boolean useMultiQuery = false;
    private boolean batchPropertyPrefetching = false;
    private VertexStepBatchFetcher vertexStepBatchFetcher;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private int txVertexCacheSize = 20000;

    public JanusGraphVertexStep(VertexStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getDirection(), originalStep.getEdgeLabels());
        originalStep.getLabels().forEach(this::addLabel);

        if (originalStep instanceof JanusGraphVertexStep) {
            JanusGraphVertexStep originalJanusGraphVertexStep = (JanusGraphVertexStep) originalStep;
            setUseMultiQuery(originalJanusGraphVertexStep.useMultiQuery);
            this.hasContainers = originalJanusGraphVertexStep.hasContainers;
            this.limit = originalJanusGraphVertexStep.limit;
        } else {
            this.hasContainers = new ArrayList<>();
            this.limit = Query.NO_LIMIT;
        }
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
        if(useMultiQuery && vertexStepBatchFetcher == null){
            vertexStepBatchFetcher = new VertexStepBatchFetcher(JanusGraphVertexStep.this::makeQuery, getReturnClass());
        }
    }

    @Override
    public void registerFutureVertexForPrefetching(Vertex futureVertex) {
        if(useMultiQuery){
            vertexStepBatchFetcher.registerFutureVertexForPrefetching(futureVertex);
        }
    }

    public void setBatchPropertyPrefetching(boolean batchPropertyPrefetching) {
        this.batchPropertyPrefetching = batchPropertyPrefetching;
    }

    public void setTxVertexCacheSize(int txVertexCacheSize) {
        this.txVertexCacheSize = txVertexCacheSize;
    }

    public <Q extends BaseVertexQuery> Q makeQuery(Q query) {
        query.labels(getEdgeLabels());
        query.direction(getDirection());
        for (final HasContainer condition : hasContainers) {
            query.has(condition.getKey(), JanusGraphPredicateUtils.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (final OrderEntry order : orders) query.orderBy(order.key, order.order);
        if (limit != BaseQuery.NO_LIMIT) query.limit(limit);
        ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
        return query;
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        Iterable<? extends JanusGraphElement> result;

        JanusGraphVertex vertexToFetchDataFor = JanusGraphTraversalUtil.getJanusGraphVertex(traverser);

        if (useMultiQuery) {
            result = vertexStepBatchFetcher.fetchData(getTraversal(), vertexToFetchDataFor);
        } else {
            final JanusGraphVertexQuery query = makeQuery(vertexToFetchDataFor.query());
            result = (Vertex.class.isAssignableFrom(getReturnClass())) ? query.vertices() : query.edges();
        }

        if (batchPropertyPrefetching && txVertexCacheSize > 1) {
            Set<JanusGraphVertex> vertices = new HashSet<>();
            for(JanusGraphElement v : result){
                vertices.add((JanusGraphVertex) v);
                if (vertices.size() >= txVertexCacheSize) {
                    break;
                }
            }

            // If there are multiple vertices then fetch the properties for all of them in a single multiQuery to
            // populate the vertex cache so subsequent queries of properties don't have to go to the storage back end
            if (vertices.size() > 1) {
                JanusGraphMultiVertexQuery propertyMultiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery(vertices);
                ((BasicVertexCentricQueryBuilder) propertyMultiQuery).profiler(queryProfiler);
                propertyMultiQuery.preFetch();
            }
        }

        return (Iterator<E>) result.iterator();
    }

    /*
    ===== HOLDER =====
     */

    private final ArrayList<HasContainer> hasContainers;
    private int limit;
    private final List<OrderEntry> orders = new ArrayList<>();

    @Override
    public void ensureAdditionalHasContainersCapacity(int additionalSize) {
        hasContainers.ensureCapacity(hasContainers.size() + additionalSize);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        HasStepFolder.splitAndP(hasContainers, hasContainer);
    }

    @Override
    public List<HasContainer> addLocalHasContainersConvertingAndPContainers(TraversalParent parent, List<HasContainer> localHasContainers) {
        throw new UnsupportedOperationException("addLocalAll is not supported for graph vertex step.");
    }

    @Override
    public List<HasContainer> addLocalHasContainersSplittingAndPContainers(TraversalParent parent, Iterable<HasContainer> has) {
        throw new UnsupportedOperationException("addLocalAll is not supported for graph vertex step.");
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(TraversalParent parent, List<HasContainer> hasContainers, String key, Order order) {
       throw new UnsupportedOperationException("localOrderBy is not supported for graph vertex step.");
    }

    @Override
    public void setLimit(int low, int high) {
        Preconditions.checkArgument(low == 0, "Offset is not supported for properties step.");
        this.limit = high;
    }

    @Override
    public void setLocalLimit(TraversalParent parent, List<HasContainer> hasContainers, int low, int high) {
        throw new UnsupportedOperationException("setLocalLimit is not supported for graph vertex step.");
    }

    @Override
    public int getLowLimit() {
        throw new UnsupportedOperationException("getLowLimit is not supported for properties step.");
    }

    @Override
    public int getLocalLowLimit(TraversalParent parent, List<HasContainer> hasContainers) {
        throw new UnsupportedOperationException("getLocalLowLimit is not supported for properties step.");
    }

    @Override
    public int getHighLimit() {
        return this.limit;
    }

    @Override
    public int getLocalHighLimit(TraversalParent parent, List<HasContainer> hasContainers) {
        throw new UnsupportedOperationException("getLocalHighLimit is not supported for graph vertex step.");
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : StringFactory.stepString(this, this.hasContainers);
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }
}
