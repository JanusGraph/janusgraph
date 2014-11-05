package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.marker.Ranging;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderByStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Order;
import com.tinkerpop.gremlin.structure.util.HasContainer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface HasStepFolder<S, E> extends Step<S, E> {

    public void addAll(Iterable<HasContainer> hasContainers);

    public void orderBy(String key, Order order);

    public void setLimit(int limit);

    public int getLimit();

    public static boolean validTitanHas(HasContainer has) {
        return TitanPredicate.Converter.supports(has.predicate);
    }

    public static boolean validTitanHas(Iterable<HasContainer> has) {
        for (HasContainer h : has) {
            if (!validTitanHas(h)) return false;
        }
        return true;
    }

    public static boolean validTitanOrder(OrderByStep ostep, Traversal traversal,
                                                boolean isVertexOrder) {
        if (!ostep.usesPropertyKey() || ostep.getPropertyValueComparators().length>1
                || !(ostep.getPropertyValueComparators()[0] instanceof Order)) return false;
        TitanTransaction tx = TitanTraversal.getTx(traversal);
        String key = (String)ostep.getPropertyKey().get();
        PropertyKey pkey = tx.getPropertyKey(key);
        if (pkey==null || !(Comparable.class.isAssignableFrom(pkey.dataType())) ) return false;
        if (isVertexOrder && pkey.cardinality()!=Cardinality.SINGLE) return false;
        return true;
    }

    public static void foldInHasContainer(final HasStepFolder titanStep, final Traversal<?, ?> traversal) {
        Step currentStep = titanStep.getNextStep();
        while (true) {
            if (TraversalHelper.isLabeled(currentStep)) break;

            if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers();
                if (validTitanHas(containers)) {
                    titanStep.addAll(containers);
                    TraversalHelper.removeStep(currentStep, traversal);
                }
            } else if (currentStep instanceof OrderByStep || currentStep instanceof OrderStep) {
                //do nothing, we can pull filters over those
            } else if (currentStep instanceof IdentityStep) {
                // do nothing, has no impact
            } else if (currentStep instanceof FilterStep) {
                // do nothing, we can rearrange filters
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    public static OrderByStep foldInLastOrderBy(final HasStepFolder titanStep, final Traversal<?, ?> traversal,
                                                boolean isVertexOrder) {
        Step currentStep = titanStep.getNextStep();
        Step lastOrder = null;
        while (true) {
            if (TraversalHelper.isLabeled(currentStep)) break;
            Step newOrder = null;
            if (currentStep instanceof OrderByStep || currentStep instanceof OrderStep) {
                newOrder = currentStep;
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
            if (lastOrder != null) TraversalHelper.removeStep(lastOrder, traversal);
            lastOrder = newOrder;
        }

        if (lastOrder instanceof OrderByStep) {
            OrderByStep<?, ?> ostep = (OrderByStep) lastOrder;
            if (validTitanOrder(ostep,traversal,isVertexOrder)) {
                titanStep.orderBy(ostep.getPropertyKey().get(), (Order) ostep.getPropertyValueComparators()[0]);
                TraversalHelper.removeStep(ostep, traversal);
                return ostep;
            }
        }
        return null;
    }


    public static <E extends Ranging> boolean foldInRange(final HasStepFolder titanStep, final Traversal<?, ?> traversal,
                                                          Class<E> rangeStepType) {
        Step nextStep = titanStep.getNextStep();
        if (rangeStepType.isInstance(nextStep)) {
            Ranging range = (Ranging) nextStep;
            int limit = QueryUtil.convertLimit(range.getHighRange());
            titanStep.setLimit(QueryUtil.mergeLimits(limit, titanStep.getLimit()));
            if (range.getLowRange() == 0) TraversalHelper.removeStep(nextStep, traversal);
            return true;
        } else return false;
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
