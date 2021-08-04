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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
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
    private final Map<String, Map<List<HasContainer>, QueryInfo>> hasLocalContainers = new LinkedHashMap<>();
    private int lowLimit = 0;
    private int highLimit = BaseQuery.NO_LIMIT;
    private final List<OrderEntry> orders = new ArrayList<>();
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private GraphCentricQuery globalQuery;
    private JanusGraphTransaction tx;


    public JanusGraphStep(final GraphStep<S, E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(), originalStep.getIds());
        originalStep.getLabels().forEach(this::addLabel);

        this.setIteratorSupplier(() -> {
            if (this.ids == null) {
                return Collections.emptyIterator();
            } else if (this.ids.length > 0) {
                final Graph graph = (Graph)traversal.asAdmin().getGraph().get();
                return iteratorList((Iterator)graph.vertices(this.ids));
            }

            buildGlobalGraphCentricQuery();
            final Multimap<Integer, GraphCentricQuery> queries = ArrayListMultimap.create();
            if (globalQuery != null && !globalQuery.getSubQuery(0).getBackendQuery().isEmpty()) {
                globalQuery.observeWith(queryProfiler.addNested(QueryProfiler.GRAPH_CENTRIC_QUERY));
                queries.put(0, globalQuery);
            } else if (hasLocalContainers.size() == 1) {
                for (Map.Entry<List<HasContainer>, QueryInfo> c : hasLocalContainers.values().iterator().next().entrySet()) {
                    GraphCentricQuery centricQuery = buildGraphCentricQuery(tx, c, queryProfiler);
                    centricQuery.observeWith(queryProfiler.addNested(QueryProfiler.GRAPH_CENTRIC_QUERY));
                    queries.put(c.getValue().getLowLimit(), centricQuery);
                }
            } else {
                globalQuery.observeWith(queryProfiler.addNested(QueryProfiler.GRAPH_CENTRIC_QUERY));
                queries.put(0, globalQuery);
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

    public GraphCentricQuery buildGlobalGraphCentricQuery() {
        if (ids == null || ids.length > 0) {
            return null;
        }

        if (globalQuery != null) {
            return globalQuery;
        }

        if (hasLocalContainers.isEmpty()) {
            Map<List<HasContainer>, QueryInfo> containers = new LinkedHashMap<>();
            containers.put(new ArrayList<>(), new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
            hasLocalContainers.put(null, containers);
        }
        tx = JanusGraphTraversalUtil.getTx(traversal);
        globalQuery = buildGlobalGraphCentricQuery(tx, queryProfiler);
        return globalQuery;
    }

    private GraphCentricQuery buildGlobalGraphCentricQuery(final JanusGraphTransaction tx, final QueryProfiler globalQueryProfiler) {
        Integer limit = null;
        for (Map<List<HasContainer>, QueryInfo> containers : hasLocalContainers.values()) {
            for (QueryInfo queryInfo : containers.values()) {
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
        }

        final JanusGraphQuery query = tx.query();
        for (Map<List<HasContainer>, QueryInfo> lc : hasLocalContainers.values()) {
            List<JanusGraphQuery> localQueries = new ArrayList<>(lc.size());
            for(final List<HasContainer> localContainers : lc.keySet()) {
                final JanusGraphQuery localQuery = tx.query();
                addConstraint(localQuery, localContainers);
                localQueries.add(localQuery);
            }
            query.or(localQueries);
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
        Map<List<HasContainer>, QueryInfo> singleLocalContainers = hasLocalContainers.values().iterator().next();
        if (hasLocalContainers.size() == 1 && singleLocalContainers.size() == 1) {
            final List<HasContainer> containers = new ArrayList<>(hasContainers);
            containers.addAll(singleLocalContainers.keySet().iterator().next());
            return StringFactory.stepString(this, Arrays.toString(this.ids), containers);
        }

        final StringBuilder sb = new StringBuilder();
        if (!hasContainers.isEmpty()) {
            sb.append(StringFactory.stepString(this, Arrays.toString(ids), hasContainers));
        }

        for (Map<List<HasContainer>, QueryInfo> localContainers : hasLocalContainers.values()) {
            if (sb.length() > 0) {
                sb.append(".");
            }
            sb.append("Or(");
            final Iterator<List<HasContainer>> itContainers = localContainers.keySet().iterator();
            sb.append(StringFactory.stepString(this, Arrays.toString(this.ids), itContainers.next()));
            while (itContainers.hasNext()) {
                sb.append(",").append(StringFactory.stepString(this, Arrays.toString(this.ids), itContainers.next()));
            }
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public void ensureAdditionalHasContainersCapacity(int additionalSize) {
        hasContainers.ensureCapacity(hasContainers.size() + additionalSize);
    }

    @Override
    public List<HasContainer> addLocalHasContainersConvertingAndPContainers(TraversalParent parent, List<HasContainer> unconvertedHasContainers){
        List<HasContainer> localHasContainers = new ArrayList<>(unconvertedHasContainers.size());
        for(HasContainer hasContainer : unconvertedHasContainers){
            localHasContainers.add(JanusGraphPredicateUtils.convert(hasContainer));
        }
        Map<List<HasContainer>, QueryInfo> hasContainers = hasLocalContainers.computeIfAbsent(parent.asStep().getId(), k -> new LinkedHashMap<>());
        hasContainers.put(localHasContainers, new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
        return localHasContainers;
    }

    @Override
    public List<HasContainer> addLocalHasContainersSplittingAndPContainers(TraversalParent parent, Iterable<HasContainer> hasContainers) {
        List<HasContainer> localHasContainers = new ArrayList<>();
        for(HasContainer hasContainer : hasContainers){
            HasStepFolder.splitAndP(localHasContainers, hasContainer);
        }
        Map<List<HasContainer>, QueryInfo> containers = hasLocalContainers.computeIfAbsent(parent.asStep().getId(), k -> new LinkedHashMap<>());
        containers.put(localHasContainers, new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
        return localHasContainers;
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(TraversalParent parent, List<HasContainer> containers, String key, Order order) {
        Map<List<HasContainer>, QueryInfo> hasContainers = hasLocalContainers.get(parent.asStep().getId());
        hasContainers.get(containers).getOrders().add(new OrderEntry(key, order));
    }

    @Override
    public void setLimit(int low, int high) {
        this.lowLimit = low;
        this.highLimit = high;
    }

    @Override
    public void setLocalLimit(TraversalParent parent, List<HasContainer> containers, int low, int high) {
        Map<List<HasContainer>, QueryInfo> hasContainers = hasLocalContainers.get(parent.asStep().getId());
        hasContainers.replace(containers, hasContainers.get(containers).setLowLimit(low).setHighLimit(high));
    }

    @Override
    public int getLowLimit() {
        return this.lowLimit;
    }

    @Override
    public int getLocalLowLimit(TraversalParent parent, List<HasContainer> containers) {
        Map<List<HasContainer>, QueryInfo> hasContainers = hasLocalContainers.get(parent.asStep().getId());
        return hasContainers.get(containers).getLowLimit();
    }

    @Override
    public int getHighLimit() {
        return this.highLimit;
    }

    @Override
    public int getLocalHighLimit(TraversalParent parent, List<HasContainer> containers) {
        Map<List<HasContainer>, QueryInfo> hasContainers = hasLocalContainers.get(parent.asStep().getId());
        return hasContainers.get(containers).getHighLimit();
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        final List<HasContainer> toReturn = new ArrayList<>(this.hasContainers);
        this.hasLocalContainers.values().forEach(c -> c.keySet().forEach(toReturn::addAll));
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
        result = 31 * result + (hasLocalContainers != null ? this.hasLocalContainers.values().stream().map(Map::hashCode).reduce(0, Integer::sum) : 0);
        result = 31 * result + lowLimit;
        result = 31 * result + highLimit;
        result = 31 * result + (orders != null ? orders.hashCode() : 0);
        return result;
    }


}

