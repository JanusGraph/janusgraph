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
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.graph.GraphCentricQuery;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.QueryInfo;
import org.janusgraph.graphdb.tinkerpop.optimize.step.util.VertexLookupUtil;
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
public class JanusGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasStepFolder<S, E>, Profiling, HasContainerHolder<S, E> {

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
                if (!Vertex.class.isAssignableFrom(getReturnClass())) {
                    return iteratorList((Iterator<E>) graph.edges(ids));
                }
                //Along with the existence of each vertex, the lookup reads what the has containers read of it anyway,
                //or without any, what the next step reads
                return iteratorList((Iterator<E>) VertexLookupUtil.vertices(this, getHasContainers(), ids));
            }

            buildGlobalGraphCentricQuery();
            final List<GraphCentricQuery> queries = new ArrayList<>();
            if (!globalQuery.getSubQuery(0).getBackendQuery().isEmpty() || hasLocalContainers.size() != 1) {
                globalQuery.observeWith(queryProfiler.addNested(QueryProfiler.GRAPH_CENTRIC_QUERY));
                queries.add(globalQuery);
            } else {
                //No index answers the or() as a whole, so each of its branches is a query of its own, which an index
                //may answer
                for (Map.Entry<List<HasContainer>, QueryInfo> c : hasLocalContainers.values().iterator().next().entrySet()) {
                    if (passesElements(c.getValue())) {
                        GraphCentricQuery centricQuery = buildGraphCentricQuery(tx, c, queryProfiler);
                        centricQuery.observeWith(queryProfiler.addNested(QueryProfiler.GRAPH_CENTRIC_QUERY));
                        queries.add(centricQuery);
                    }
                }
            }

            if (queries.isEmpty()) {
                //An or() of which no branch lets an element through: there is nothing to read
                return Collections.emptyIterator();
            }
            final GraphCentricQueryBuilder builder = (GraphCentricQueryBuilder) tx.query();
            final List<Iterator<E>> responses = new ArrayList<>();
            queries.forEach(q -> executeGraphCentricQuery(builder, responses, q));

            if (orders.isEmpty()) {
                return new MultiDistinctUnorderedIterator<>(lowLimit, highLimit, responses);
            } else {
                //Every query which may return elements is ordered by the step's orders, so they come in the order of
                //the OrderList which those queries share, which compares their values as JanusGraph does, implicit keys
                //such as ~nid included, and so does their merge. A query which returns none, such as one with a limit
                //of 0 or a condition no element can hold, has no order
                final OrderList order = queries.stream().map(GraphCentricQuery::getOrder)
                    .filter(o -> !o.isEmpty()).findFirst().orElse(OrderList.NO_ORDER);
                assert queries.stream().allMatch(q -> q.getOrder().isEmpty() || q.getOrder().equals(order));
                return new MultiDistinctOrderedIterator<>(lowLimit, highLimit, responses,
                    (e1, e2) -> order.compare((JanusGraphElement) e1, (JanusGraphElement) e2));
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
        final JanusGraphQuery query = tx.query();
        for (Map<List<HasContainer>, QueryInfo> lc : hasLocalContainers.values()) {
            List<JanusGraphQuery> localQueries = new ArrayList<>(lc.size());
            for (final Map.Entry<List<HasContainer>, QueryInfo> localContainers : lc.entrySet()) {
                if (passesElements(localContainers.getValue())) {
                    final JanusGraphQuery localQuery = tx.query();
                    addConstraint(localQuery, localContainers);
                    localQueries.add(localQuery);
                }
            }
            if (localQueries.isEmpty()) {
                //No element gets through any branch of the or()
                return GraphCentricQuery.emptyQuery(
                    Vertex.class.isAssignableFrom(this.returnClass) ? ElementCategory.VERTEX : ElementCategory.EDGE);
            }
            query.or(localQueries);
        }
        for (final OrderEntry order : orders) query.orderBy(order.key, order.order);
        query.limit(highLimit);
        return buildGraphCentricQuery(query, globalQueryProfiler);
    }

    //TinkerPop runs a branch of an or() for each element on its own, so a range() or limit() of the branch lets the
    //element through, unless it skips it or its limit is 0
    private static boolean passesElements(final QueryInfo branch) {
        return branch.getLowLimit() == 0 && branch.getHighLimit() != 0;
    }

    private void addConstraint(final JanusGraphQuery query,
            final Entry<List<HasContainer>, QueryInfo> localContainers) {
        for (final HasContainer condition : hasContainers) {
            query.has(condition.getKey(), JanusGraphPredicateUtils.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (final HasContainer condition : localContainers.getKey()) {
            query.has(condition.getKey(), JanusGraphPredicateUtils.convert(condition.getBiPredicate()), condition.getValue());
        }
        //and an order() of the branch where each of its keys has a value
        for (final OrderEntry order : localContainers.getValue().getOrders()) {
            query.has(order.key);
        }
    }

    private GraphCentricQuery buildGraphCentricQuery(final JanusGraphTransaction tx,
            final Entry<List<HasContainer>, QueryInfo> containers, final QueryProfiler queryProfiler) {
        final JanusGraphQuery query = tx.query();
        addConstraint(query, containers);
        for (final OrderEntry order : orders) query.orderBy(order.key, order.order);
        query.limit(highLimit);
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
            final GraphCentricQuery query) {
        final QueryProfiler profiler = query.getProfiler();
        final Class<? extends JanusGraphElement> graphClass = Vertex.class.isAssignableFrom(this.returnClass) ? JanusGraphVertex.class: JanusGraphEdge.class;
        responses.add(new ProfiledIterator(profiler, () -> builder.iterables(query, graphClass).iterator()));
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
            //An or() which lets no element through has no branch
            final Iterator<List<HasContainer>> itContainers = localContainers.keySet().iterator();
            if (itContainers.hasNext()) {
                sb.append(StringFactory.stepString(this, Arrays.toString(this.ids), itContainers.next()));
            }
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

    /**
     * Folds in an or() of which no branch lets an element through, as each of them skips the element, so that the step
     * returns no element
     *
     * @param parent the or()
     */
    public void addLocalHasContainersOfNoBranch(TraversalParent parent) {
        hasLocalContainers.computeIfAbsent(parent.asStep().getId(), k -> new LinkedHashMap<>());
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
        final List<A> list = new ArrayList<>();
        do {
            final A e = iterator.next();
            if (holdsConditions(e)) {
                list.add(e);
            }
        } while (iterator.hasNext());
        return list.iterator();
    }

    /*
     * Whether the element of a given id holds the conditions of the step, as its query would check them: its own, and
     * those of a branch of each or() which lets the element through, one whose range doesn't skip it and where it has
     * a value of each key of the branch's order
     */
    private boolean holdsConditions(final Element element) {
        if (!HasContainer.testAll(element, hasContainers)) {
            return false;
        }
        for (final Map<List<HasContainer>, QueryInfo> or : hasLocalContainers.values()) {
            boolean holdsABranch = false;
            for (final Entry<List<HasContainer>, QueryInfo> branch : or.entrySet()) {
                if (passesElements(branch.getValue()) && HasContainer.testAll(element, branch.getKey())
                        && branch.getValue().getOrders().stream()
                            .allMatch(order -> element.properties(order.key).hasNext())) {
                    holdsABranch = true;
                    break;
                }
            }
            if (!holdsABranch) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (hasContainers != null ? this.hasContainers.hashCode() : 0);
        //each or() counts, also one of no branches, whose map adds nothing of its own
        result = 31 * result + (hasLocalContainers != null ? this.hasLocalContainers.values().stream()
            .map(m -> 31 * m.hashCode() + 1).reduce(0, Integer::sum) : 0);
        result = 31 * result + lowLimit;
        result = 31 * result + highLimit;
        result = 31 * result + (orders != null ? orders.hashCode() : 0);
        return result;
    }


}

