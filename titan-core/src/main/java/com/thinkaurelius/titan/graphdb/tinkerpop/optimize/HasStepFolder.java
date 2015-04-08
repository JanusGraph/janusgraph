package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Ranging;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Order;

import java.util.Comparator;
import java.util.List;

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

    public static boolean validTitanOrder(OrderStep ostep, Traversal rootTraversal,
                                                boolean isVertexOrder) {
        for (Comparator comp : (List<Comparator>)ostep.getComparators()) {
            if (!(comp instanceof ElementValueComparator)) return false;
            ElementValueComparator evc = (ElementValueComparator)comp;
            if (!(evc.getValueComparator() instanceof Order)) return false;

            TitanTransaction tx = TitanTraversalUtil.getTx(rootTraversal);
            String key = evc.getPropertyKey();
            PropertyKey pkey = tx.getPropertyKey(key);
            if (pkey==null || !(Comparable.class.isAssignableFrom(pkey.dataType())) ) return false;
            if (isVertexOrder && pkey.cardinality()!=Cardinality.SINGLE) return false;
        }
        return true;
    }

    public static void foldInHasContainer(final HasStepFolder titanStep, final Traversal.Admin<?, ?> traversal) {
        Step currentStep = titanStep.getNextStep();
        while (true) {
            if (currentStep.getLabel().isPresent()) break;

            if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers();
                if (validTitanHas(containers)) {
                    titanStep.addAll(containers);
                    addLabeledStepAsIdentity(currentStep, traversal);
                    traversal.removeStep(currentStep);
                }
            } else if (currentStep instanceof OrderStep) {
                //do nothing, we can pull filters over those
            } else if (currentStep instanceof IdentityStep) {
                // do nothing, has no impact
            } else if (currentStep instanceof HasStep) {
                // do nothing, we can rearrange filters
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    public static void addLabeledStepAsIdentity(Step<?,?> currentStep, final Traversal.Admin<?, ?> traversal) {
        currentStep.getLabel().ifPresent(label -> {
            final IdentityStep identityStep = new IdentityStep<>(traversal);
            identityStep.setLabel(label);
            TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
        });
    }

    public static void foldInOrder(final HasStepFolder titanStep, final Traversal.Admin<?, ?> traversal,
                                                final Traversal<?,?> rootTraversal, boolean isVertexOrder) {
        Step currentStep = titanStep.getNextStep();
        OrderStep lastOrder = null;
        while (true) {
            if (currentStep instanceof OrderStep) {
                if (lastOrder!=null) { //Previous orders are rendered irrelevant by next order (since re-ordered)
                    addLabeledStepAsIdentity(lastOrder, traversal);
                    traversal.removeStep(lastOrder);
                }
                lastOrder = (OrderStep)currentStep;
            } else if (currentStep instanceof IdentityStep) {
                // do nothing, can be skipped
            } else if (currentStep instanceof HasStep) {
                // do nothing, can be skipped
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        if (lastOrder!=null && lastOrder instanceof OrderStep) {
            if (validTitanOrder(lastOrder,rootTraversal,isVertexOrder)) {
                //Add orders to HasStepFolder
                for (Comparator comp : (List<Comparator>)lastOrder.getComparators()) {
                    ElementValueComparator evc = (ElementValueComparator)comp;
                    titanStep.orderBy(evc.getPropertyKey(),(Order)evc.getValueComparator());
                }
                addLabeledStepAsIdentity(lastOrder, traversal);
                traversal.removeStep(lastOrder);
            }
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

    public static <E extends Ranging> void foldInRange(final HasStepFolder titanStep, final Traversal.Admin<?, ?> traversal) {
        Step nextStep = TitanTraversalUtil.getNextNonIdentityStep(titanStep);

        if (nextStep instanceof RangeStep) {
            RangeStep range = (RangeStep)nextStep;
            int limit = QueryUtil.convertLimit(range.getHighRange());
            titanStep.setLimit(QueryUtil.mergeLimits(limit, titanStep.getLimit()));
            if (range.getLowRange() == 0) { //Range can be removed since there is no offset
                addLabeledStepAsIdentity(nextStep, traversal);
                traversal.removeStep(nextStep);
            }
        }
    }



}
