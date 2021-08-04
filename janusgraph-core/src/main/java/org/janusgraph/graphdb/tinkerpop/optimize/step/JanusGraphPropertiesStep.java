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
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphProperty;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphPropertiesStep<E> extends PropertiesStep<E> implements HasStepFolder<Element, E>, Profiling, MultiQueriable<Element,E> {

    private boolean useMultiQuery = false;
    private Set<Vertex> verticesToPrefetch = new HashSet<>();
    private Map<JanusGraphVertex, Iterable<? extends JanusGraphProperty>> multiQueryResults = null;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;

    public JanusGraphPropertiesStep(PropertiesStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnType(), originalStep.getPropertyKeys());
        originalStep.getLabels().forEach(this::addLabel);

        if (originalStep instanceof JanusGraphPropertiesStep) {
            JanusGraphPropertiesStep originalJanusGraphPropertiesStep = (JanusGraphPropertiesStep) originalStep;
            this.useMultiQuery = originalJanusGraphPropertiesStep.useMultiQuery;
            this.hasContainers = originalJanusGraphPropertiesStep.hasContainers;
            this.limit = originalJanusGraphPropertiesStep.limit;
        } else {
            this.hasContainers = new ArrayList<>();
            this.limit = Query.NO_LIMIT;
        }
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
    }

    @Override
    public void registerFutureVertexForPrefetching(Vertex futureVertex) {
        verticesToPrefetch.add(futureVertex);
    }

    private <Q extends BaseVertexQuery> Q makeQuery(Q query) {
        final String[] keys = getPropertyKeys();
        query.keys(keys);
        for (final HasContainer condition : hasContainers) {
            query.has(condition.getKey(), JanusGraphPredicateUtils.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (final OrderEntry order : orders) query.orderBy(order.key, order.order);
        if (limit != BaseQuery.NO_LIMIT) query.limit(limit);
        ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
        return query;
    }

    private Iterator<E> convertIterator(Iterable<? extends JanusGraphProperty> iterable) {
        if (getReturnType().forProperties()) {
            return (Iterator<E>) iterable.iterator();
        }
        assert getReturnType().forValues();
        return (Iterator<E>) Iterators.transform(iterable.iterator(), Property::value);
    }

    /**
     * This initialisation method is called when an attempt to retrieve a vertex from the cached multiQuery results
     * doesn't find an entry.
     */
    private void prefetchNextBatch() {
        final JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(getTraversal()).multiQuery();
        multiQuery.addAllVertices(verticesToPrefetch);
        verticesToPrefetch.clear();
        makeQuery(multiQuery);

        try {
            multiQueryResults = multiQuery.properties();
        } catch (JanusGraphException janusGraphException) {
            if (janusGraphException.isCausedBy(InterruptedException.class)) {
                throw new TraversalInterruptedException();
            }
        }
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Element> traverser) {
        if (useMultiQuery && traverser.get() instanceof Vertex) {
            if (multiQueryResults == null || !multiQueryResults.containsKey(traverser.get())) {
                prefetchNextBatch();
            }
            return convertIterator(multiQueryResults.get(traverser.get()));
        } else if (traverser.get() instanceof JanusGraphVertex || traverser.get() instanceof WrappedVertex) {
            final JanusGraphVertexQuery query = makeQuery((JanusGraphTraversalUtil.getJanusGraphVertex(traverser)).query());
            return convertIterator(query.properties());
        } else {
            //It is some other element (edge or vertex property)
            Iterator<E> iterator;
            if (getReturnType().forValues()) {
                assert orders.isEmpty() && hasContainers.isEmpty();
                iterator = traverser.get().values(getPropertyKeys());
            } else {
                //this asks for properties
                assert orders.isEmpty();
                Iterator<? extends Property<?>> propertiesIt = traverser.get().properties(getPropertyKeys());
                if(hasContainers.isEmpty()){
                    iterator = (Iterator<E>) propertiesIt;
                } else {
                    List<E> properties = new LinkedList<>();
                    propertiesIt.forEachRemaining(e -> {
                        if(HasContainer.testAll(e, hasContainers)){
                            properties.add((E) e);
                        }
                    });
                    iterator = properties.iterator();
                }
            }
            if (limit!=Query.NO_LIMIT) iterator = Iterators.limit(iterator,limit);
            return iterator;
        }
    }

    /*
    ===== HOLDER =====
     */

    private final ArrayList<HasContainer> hasContainers;
    private int limit;
    private final List<HasStepFolder.OrderEntry> orders = new ArrayList<>();

    @Override
    public void ensureAdditionalHasContainersCapacity(int additionalSize) {
        hasContainers.ensureCapacity(hasContainers.size() + additionalSize);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        hasContainers.add(hasContainer);
    }

    @Override
    public List<HasContainer> addLocalHasContainersConvertingAndPContainers(TraversalParent parent, List<HasContainer> localHasContainers) {
        throw new UnsupportedOperationException("addLocalAll is not supported for properties step.");
    }

    @Override
    public List<HasContainer> addLocalHasContainersSplittingAndPContainers(TraversalParent parent, Iterable<HasContainer> has) {
        throw new UnsupportedOperationException("addLocalAll is not supported for properties step.");
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new HasStepFolder.OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(TraversalParent parent, List<HasContainer> hasContainers, String key, Order order) {
        throw new UnsupportedOperationException("LocalOrderBy is not supported for properties step.");
    }

    @Override
    public void setLimit(int low, int high) {
        Preconditions.checkArgument(low == 0, "Offset is not supported for properties step.");
        this.limit = high;
    }

    @Override
    public void setLocalLimit(TraversalParent parent, List<HasContainer> hasContainers, int low, int high) {
        throw new UnsupportedOperationException("LocalLimit is not supported for properties step.");
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
        throw new UnsupportedOperationException("getLocalHighLimit is not supported for properties step.");
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
