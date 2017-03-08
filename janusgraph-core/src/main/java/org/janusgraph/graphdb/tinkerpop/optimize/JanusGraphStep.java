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

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.query.BaseQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasStepFolder<S, E>, Profiling, HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();
    private int limit = BaseQuery.NO_LIMIT;
    private List<OrderEntry> orders = new ArrayList<>();
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;


    public JanusGraphStep(final GraphStep<S, E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(), originalStep.getIds());
        originalStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> {
            if (this.ids != null && this.ids.length > 0) {
                final Graph graph = (Graph)traversal.asAdmin().getGraph().get();
                return iteratorList((Iterator)graph.vertices(this.ids));
            }
            JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(traversal);
            JanusGraphQuery query = tx.query();
            for (HasContainer condition : hasContainers) {
                query.has(condition.getKey(), JanusGraphPredicate.Converter.convert(condition.getBiPredicate()), condition.getValue());
            }
            for (OrderEntry order : orders) query.orderBy(order.key, order.order);
            if (limit != BaseQuery.NO_LIMIT) query.limit(limit);
            ((GraphCentricQueryBuilder) query).profiler(queryProfiler);
            return Vertex.class.isAssignableFrom(this.returnClass) ? query.vertices().iterator() : query.edges().iterator();
        });
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ?
                super.toString() : StringFactory.stepString(this, Arrays.toString(this.ids), this.hasContainers);
    }

    @Override
    public void addAll(Iterable<HasContainer> has) {
        Iterables.addAll(hasContainers, has);
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key, order));
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getLimit() {
        return this.limit;
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return this.hasContainers;
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.addAll(Collections.singleton(hasContainer));
    }

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<E>();
        while (iterator.hasNext()) {
            final E e = iterator.next();
            if (HasContainer.testAll(e, this.hasContainers))
                list.add(e);
        }
        return list.iterator();
    }
}

