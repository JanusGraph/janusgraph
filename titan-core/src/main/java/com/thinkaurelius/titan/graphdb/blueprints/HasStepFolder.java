package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Order;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface HasStepFolder<S,E> extends Step<S,E> {

    public void addAll(Iterable<HasContainer> hasContainers);

    public void orderBy(String key, Order order);

    public void setLimit(int limit);

    public int getLimit();

    public static boolean validTitanHas(HasContainer has) {
        return TitanPredicate.Converter.supports(has.predicate);
    }

    public static boolean validTitanHas(Iterable<HasContainer> has) {
        for (HasContainer h : has) { if (!validTitanHas(h)) return false; }
        return true;
    }

    public static boolean validTitanHas(HasContainerHolder has) {
        return validTitanHas(has.getHasContainers());
    }

    public static void foldInHasContainer(final HasStepFolder titanStep, final Traversal<?, ?> traversal) {
        Step currentStep = titanStep.getNextStep();
        boolean skippedOrders = false;
        while (true) {
            if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;

            if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers();
                if (validTitanHas(containers)) {
                    titanStep.addAll(containers);
                    TraversalHelper.removeStep(currentStep, traversal);
                }
            } else if (currentStep instanceof OrderByStep && !skippedOrders) {
                OrderByStep ostep = (OrderByStep)currentStep;
                if (ostep.getElementValueComparator() instanceof Order) {
                    titanStep.orderBy(ostep.getElementKey(),(Order)ostep.getElementValueComparator());
                    TraversalHelper.removeStep(currentStep, traversal);
                } else skippedOrders = true;
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else if (currentStep instanceof FilterStep) {
                // do nothing
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    public static class OrderEntry {

        public final String key;
        public final Order order;

        public OrderEntry(String key, Order order) {
            this.key = key;
            this.order = order;
        }
    }

}
