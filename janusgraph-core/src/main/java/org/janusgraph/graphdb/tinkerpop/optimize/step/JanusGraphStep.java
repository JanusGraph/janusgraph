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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.graph.GraphCentricQuery;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.QueryInfo;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.janusgraph.graphdb.util.MultiDistinctOrderedIterator;
import org.janusgraph.graphdb.util.MultiDistinctUnorderedIterator;
import org.janusgraph.graphdb.util.ProfiledIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasStepFolder<S, E>, Profiling, HasContainerHolder {

    private final ArrayList<HasContainer> hasContainers = new ArrayList<>();
    private final Map<List<HasContainer>, QueryInfo> hasLocalContainers = new LinkedHashMap<>();
    private int lowLimit = 0;
    private int highLimit = BaseQuery.NO_LIMIT;
    private final List<OrderEntry> orders = new ArrayList<>();
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;


    public JanusGraphStep(final GraphStep<S, E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(), originalStep.getIds());
        originalStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> {
            if (this.ids == null) {
                return Collections.emptyIterator();
            }
            else if (this.ids.length > 0) {
                final Graph graph = (Graph)traversal.asAdmin().getGraph().get();
                return iteratorList((Iterator)graph.vertices(this.ids));
            }
            if (hasLocalContainers.isEmpty()) {
                hasLocalContainers.put(new ArrayList<>(), new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
            }
            final JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(traversal);

            final GraphCentricQuery globalQuery = buildGlobalGraphCentricQuery(tx, queryProfiler);

            final Multimap<Integer, GraphCentricQuery> queries = ArrayListMultimap.create();
            if (globalQuery != null && !globalQuery.getSubQuery(0).getBackendQuery().isEmpty()) {
                globalQuery.observeWith(queryProfiler.addNested(QueryProfiler.GRAPH_CENTRIC_QUERY));
                queries.put(0, globalQuery);
            } else {
                for (Map.Entry<List<HasContainer>, QueryInfo> c : hasLocalContainers.entrySet()) {
                    GraphCentricQuery centricQuery = buildGraphCentricQuery(tx, c, queryProfiler);
                    centricQuery.observeWith(queryProfiler.addNested(QueryProfiler.GRAPH_CENTRIC_QUERY));
                    queries.put(c.getValue().getLowLimit(), centricQuery);
                }
            }

            final GraphCentricQueryBuilder builder = (GraphCentricQueryBuilder) tx.query();
            final List<Iterator<E>> responses = new ArrayList<>();
            queries.entries().forEach(q ->  executeGraphCentricQuery(builder, responses, q));

            if (orders.isEmpty()) {
                return new MultiDistinctUnorderedIterator<>(lowLimit, highLimit, responses);
            } else {
                return new MultiDistinctOrderedIterator<>(lowLimit, highLimit, responses, orders);
            }
        });
    }

    private GraphCentricQuery buildGlobalGraphCentricQuery(final JanusGraphTransaction tx, final QueryProfiler globalQueryProfiler) {
        Integer limit = null;
        for (QueryInfo queryInfo : hasLocalContainers.values()) {
            if (queryInfo.getLowLimit() > 0 || orders.isEmpty() && !queryInfo.getOrders().isEmpty()) {
                return null;
            }
            final int currentHighLimit = queryInfo.getHighLimit();
            if (limit == null) {
                limit = currentHighLimit;
            } else if (currentHighLimit < highLimit && !limit.equals(currentHighLimit)) {
                return null;
            }
        }
        final JanusGraphQuery query = tx.query();
        for(final List<HasContainer> localContainers : hasLocalContainers.keySet()) {
            final JanusGraphQuery localQuery = tx.query();
            addConstraint(localQuery, localContainers);
            query.or(localQuery);
        }
        for (final OrderEntry order : orders) query.orderBy(order.key, order.order);
        query.limit(Math.min(limit, highLimit));
        return buildGraphCentricQuery(query, globalQueryProfiler);
    }

    private void addConstraint(final JanusGraphQuery query, final List<HasContainer> localContainers) {
        for (final HasContainer condition : hasContainers) {
            query.has(condition.getKey(), JanusGraphPredicateUtils.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (final HasContainer condition : localContainers) {
            query.has(condition.getKey(), JanusGraphPredicateUtils.convert(condition.getBiPredicate()), condition.getValue());
        }
    }

    private GraphCentricQuery buildGraphCentricQuery(final JanusGraphTransaction tx,
            final Entry<List<HasContainer>, QueryInfo> containers, final QueryProfiler queryProfiler) {
        final JanusGraphQuery query = tx.query();
        addConstraint(query, containers.getKey());
        final List<OrderEntry> realOrders = orders.isEmpty() ? containers.getValue().getOrders() : orders;
        for (final OrderEntry order : realOrders) query.orderBy(order.key, order.order);
        if (highLimit != BaseQuery.NO_LIMIT || containers.getValue().getHighLimit() != BaseQuery.NO_LIMIT) query.limit(Math.min(containers.getValue().getHighLimit(), highLimit));
        return buildGraphCentricQuery(query, queryProfiler);
    }

    private GraphCentricQuery buildGraphCentricQuery(JanusGraphQuery query, final QueryProfiler queryProfiler) {
        Preconditions.checkArgument(query instanceof GraphCentricQueryBuilder);
        final QueryProfiler optProfiler = queryProfiler.addNested(QueryProfiler.CONSTRUCT_GRAPH_CENTRIC_QUERY);
        optProfiler.startTimer();
        final GraphCentricQueryBuilder centricQueryBuilder = ((GraphCentricQueryBuilder) query);
        if (traversal.getEndStep() instanceof CountGlobalStep) {
            centricQueryBuilder.disableSmartLimit();
        }
        final GraphCentricQuery graphCentricQuery = centricQueryBuilder.constructQueryWithoutProfile(Vertex.class.isAssignableFrom(this.returnClass) ? ElementCategory.VERTEX: ElementCategory.EDGE);
        optProfiler.stopTimer();
        return graphCentricQuery;
    }

    private void executeGraphCentricQuery(final GraphCentricQueryBuilder builder, final List<Iterator<E>> responses,
            final Entry<Integer, GraphCentricQuery> entry) {
        final GraphCentricQuery query = entry.getValue();
        final QueryProfiler profiler = query.getProfiler();
        final Class<? extends JanusGraphElement> graphClass = Vertex.class.isAssignableFrom(this.returnClass) ? JanusGraphVertex.class: JanusGraphEdge.class;
        final ProfiledIterator iterator = new ProfiledIterator(profiler, () -> builder.iterables(query, graphClass).iterator());
        long i = 0;
        while (i < entry.getKey() && iterator.hasNext()) {
            iterator.next();
            i++;
        }
        responses.add(iterator);
    }

    @Override
    public String toString() {
        if (hasLocalContainers.isEmpty() && hasContainers.isEmpty()){
            return super.toString();
        }
        if (hasLocalContainers.isEmpty()) {
            return StringFactory.stepString(this, Arrays.toString(this.ids), hasContainers);
        }
        if (hasLocalContainers.size() == 1){
            final List<HasContainer> containers = new ArrayList<>(hasContainers);
            containers.addAll(hasLocalContainers.keySet().iterator().next());
            return StringFactory.stepString(this, Arrays.toString(this.ids), containers);
        }
        final StringBuilder sb = new StringBuilder();
        if (!hasContainers.isEmpty()) {
            sb.append(StringFactory.stepString(this, Arrays.toString(ids), hasContainers)).append(".");
        }
        sb.append("Or(");
        final Iterator<List<HasContainer>> itContainers = this.hasLocalContainers.keySet().iterator();
        sb.append(StringFactory.stepString(this, Arrays.toString(this.ids), itContainers.next()));
        while(itContainers.hasNext()){
            sb.append(",").append(StringFactory.stepString(this, Arrays.toString(this.ids), itContainers.next()));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void ensureAdditionalHasContainersCapacity(int additionalSize) {
        hasContainers.ensureCapacity(hasContainers.size() + additionalSize);
    }

    @Override
    public List<HasContainer> addLocalHasContainersConvertingAndPContainers(List<HasContainer> unconvertedHasContainers){
        List<HasContainer> localHasContainers = new ArrayList<>(unconvertedHasContainers.size());
        for(HasContainer hasContainer : unconvertedHasContainers){
            localHasContainers.add(JanusGraphPredicateUtils.convert(hasContainer));
        }
        hasLocalContainers.put(localHasContainers, new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
        return localHasContainers;
    }

    @Override
    public List<HasContainer> addLocalHasContainersSplittingAndPContainers(Iterable<HasContainer> hasContainers) {
        List<HasContainer> localHasContainers = new ArrayList<>();
        for(HasContainer hasContainer : hasContainers){
            HasStepFolder.splitAndP(localHasContainers, hasContainer);
        }
        hasLocalContainers.put(localHasContainers, new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
        return localHasContainers;
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(List<HasContainer> containers, String key, Order order) {
       hasLocalContainers.get(containers).getOrders().add(new OrderEntry(key, order));
    }

    @Override
    public void setLimit(int low, int high) {
        this.lowLimit = low;
        this.highLimit = high;
    }

    @Override
    public void setLocalLimit(List<HasContainer> containers, int low, int high) {
        hasLocalContainers.replace(containers, hasLocalContainers.get(containers).setLowLimit(low).setHighLimit(high));
    }

    @Override
    public int getLowLimit() {
        return this.lowLimit;
    }

    @Override
    public int getLocalLowLimit(List<HasContainer> containers) {
        return hasLocalContainers.get(containers).getLowLimit();
    }

    @Override
    public int getHighLimit() {
        return this.highLimit;
    }

    @Override
    public int getLocalHighLimit(List<HasContainer> containers) {
        return hasLocalContainers.get(containers).getHighLimit();
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        final List<HasContainer> toReturn = new ArrayList<>(this.hasContainers);
        this.hasLocalContainers.keySet().forEach(toReturn::addAll);
        return toReturn;
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        HasStepFolder.splitAndP(hasContainers, hasContainer);
    }

    public List<OrderEntry> getOrders() {
        return orders;
    }

    private <A extends Element> Iterator<A> iteratorList(final Iterator<A> iterator) {
        if(!iterator.hasNext()){
            return Collections.emptyIterator();
        }
        List<HasContainer> hasContainers = getHasContainers();
        final List<A> list = new ArrayList<>(hasContainers.size());
        do {
            final A e = iterator.next();
            if (HasContainer.testAll(e, hasContainers)){
                list.add(e);
            }
        } while (iterator.hasNext());
        return list.iterator();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (hasContainers != null ? this.hasContainers.hashCode() : 0);
        result = 31 * result + (hasLocalContainers != null ? this.hasLocalContainers.hashCode() : 0);
        result = 31 * result + lowLimit;
        result = 31 * result + highLimit;
        result = 31 * result + (orders != null ? orders.hashCode() : 0);
        return result;
    }


}

