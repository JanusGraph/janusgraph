package com.thinkaurelius.titan.graphdb.blueprints;

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
import com.tinkerpop.gremlin.structure.Order;
import com.tinkerpop.gremlin.structure.util.HasContainer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface HasStepFolder<S,E> extends Step<S,E> {

    public void addAll(Iterable<HasContainer> hasContainers);

    public void orderBy(String key, Order order);

    public void setLimit(int limit);

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
        boolean skippedSteps = false;
        while (true) {
            if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;

            if (currentStep instanceof HasContainerHolder) {
                HasContainerHolder hasHolder = (HasContainerHolder) currentStep;
                if (validTitanHas(hasHolder)) {
                    titanStep.addAll(hasHolder.getHasContainers());
                    TraversalHelper.removeStep(currentStep, traversal);
                } else skippedSteps = true;
            } else if (currentStep instanceof OrderByStep) {
                OrderByStep ostep = (OrderByStep)currentStep;
                if (ostep.getComparator() instanceof Order) {
                    titanStep.orderBy(ostep.getElementKey(),(Order)ostep.getComparator());
                    TraversalHelper.removeStep(currentStep, traversal);
                } else skippedSteps = true;
            } else if (currentStep instanceof RangeStep && !skippedSteps) {
            //can only apply limit if we haven't skipped any filters or orders
                RangeStep rstep = (RangeStep)currentStep;
                long high = rstep.getHighRange();

                //TODO: remove +1 once semantics is updated
                if (high>=Integer.MAX_VALUE || high+1>=Integer.MAX_VALUE) titanStep.setLimit(Integer.MAX_VALUE);
                else titanStep.setLimit((int)high+1);

                if (rstep.getLowRange()==0) TraversalHelper.removeStep(currentStep, traversal);
                break; //Cannot optimize beyond limit
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else if (currentStep instanceof FilterStep) {
                skippedSteps = true;
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
