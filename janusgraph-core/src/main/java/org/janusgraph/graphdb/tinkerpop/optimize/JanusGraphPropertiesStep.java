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

package org.janusgraph.graphdb.tinkerpop.optimize;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.core.*;
import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphPropertiesStep<E> extends PropertiesStep<E> implements HasStepFolder<Element, E>, Profiling, MultiQueriable<Element,E> {

    private boolean initialized = false;
    private boolean useMultiQuery = false;
    private Map<JanusGraphVertex, Iterable<? extends JanusGraphProperty>> multiQueryResults = null;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;

    public JanusGraphPropertiesStep(PropertiesStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnType(), originalStep.getPropertyKeys());
        originalStep.getLabels().forEach(this::addLabel);
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
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

    private void initialize() {
        assert !initialized;
        initialized = true;
        assert getReturnType().forProperties() || (orders.isEmpty() && hasContainers.isEmpty());

        if (!starts.hasNext()) throw FastNoSuchElementException.instance();
        final List<Traverser.Admin<Element>> elements = new ArrayList<>();
        starts.forEachRemaining(elements::add);
        starts.add(elements.iterator());
        assert elements.size() > 0;

        useMultiQuery = useMultiQuery && elements.stream().allMatch(e -> e.get() instanceof Vertex);

        if (useMultiQuery) {
            initializeMultiQuery(elements);
        }
    }

    /**
     * This initialisation method is called the first time this instance is used and also when
     * an attempt to retrieve a vertex from the cached multiQuery results doesn't find an entry.
     * @param vertices A list of vertices with which to initialise the multiQuery
     */
    private void initializeMultiQuery(final List<Traverser.Admin<Element>> vertices) {
        assert vertices.size() > 0;
        final JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery();
        vertices.forEach(v -> multiQuery.addVertex((Vertex)v.get()));
        makeQuery(multiQuery);

        Map<JanusGraphVertex, Iterable<? extends JanusGraphProperty>> results = multiQuery.properties();
        if (multiQueryResults == null) {
            multiQueryResults = results;
        } else {
            multiQueryResults.putAll(results);
        }
        initialized = true;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Element> traverser) {
        if (useMultiQuery) { //it is guaranteed that all elements are vertices
            if (multiQueryResults == null || !multiQueryResults.containsKey(traverser.get())) {
                initializeMultiQuery(Collections.singletonList(traverser));
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
                iterator = (Iterator<E>) traverser.get().properties(getPropertyKeys());
                if (!hasContainers.isEmpty()) {
                    List<E> properties = new LinkedList<>();
                    iterator.forEachRemaining(e -> {
                        if(HasContainer.testAll(e, hasContainers)){
                            properties.add(e);
                        }
                    });
                    iterator = properties.iterator();
                }
            }
            if (limit!=Query.NO_LIMIT) iterator = Iterators.limit(iterator,limit);
            return iterator;
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public JanusGraphPropertiesStep<E> clone() {
        final JanusGraphPropertiesStep<E> clone = (JanusGraphPropertiesStep<E>) super.clone();
        clone.initialized = false;
        return clone;
    }

    /*
    ===== HOLDER =====
     */

    private final List<HasContainer> hasContainers;
    private int limit;
    private final List<HasStepFolder.OrderEntry> orders = new ArrayList<>();


    @Override
    public void addAll(Iterable<HasContainer> has) {
        Iterables.addAll(hasContainers, has);
    }

    @Override
    public List<HasContainer> addLocalAll(Iterable<HasContainer> has) {
        throw new UnsupportedOperationException("addLocalAll is not supported for properties step.");
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new HasStepFolder.OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(List<HasContainer> hasContainers, String key, Order order) {
       throw new UnsupportedOperationException("LocalOrderBy is not supported for properties step.");
    }

    @Override
    public void setLimit(int low, int high) {
        Preconditions.checkArgument(low == 0, "Offset is not supported for properties step.");
        this.limit = high;
    }

    @Override
    public void setLocalLimit(List<HasContainer> hasContainers, int low, int high) {
        throw new UnsupportedOperationException("LocalLimit is not supported for properties step.");
    }

    @Override
    public int getLowLimit() {
        throw new UnsupportedOperationException("getLowLimit is not supported for properties step.");
    }

    @Override
    public int getLocalLowLimit(List<HasContainer> hasContainers) {
        throw new UnsupportedOperationException("getLocalLowLimit is not supported for properties step.");
    }

    @Override
    public int getHighLimit() {
        return this.limit;
    }

    @Override
    public int getLocalHighLimit(List<HasContainer> hasContainers) {
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
