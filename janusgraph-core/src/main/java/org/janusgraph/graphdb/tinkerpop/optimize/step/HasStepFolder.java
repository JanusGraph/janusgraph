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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface HasStepFolder<S, E> extends Step<S, E> {

    /**
     * Optional method which provides a hint to future has containers additions.
     * The method will optionally resize the underlying array for future addition to ensure that it can hold additional
     * elements. I.e. it will ensure that we can add `additionalSize` containers without any resizes.
     */
    void ensureAdditionalHasContainersCapacity(int additionalSize);

    void addHasContainer(final HasContainer hasContainer);

    List<HasContainer> addLocalHasContainersConvertingAndPContainers(TraversalParent parent, List<HasContainer> localHasContainers);

    List<HasContainer> addLocalHasContainersSplittingAndPContainers(TraversalParent parent, Iterable<HasContainer> has);

    void orderBy(String key, Order order);

    void localOrderBy(TraversalParent parent, List<HasContainer> hasContainers, String key, Order order);

    void setLimit(int low, int high);

    void setLocalLimit(TraversalParent parent, List<HasContainer> hasContainers, int low, int high);

    int getLowLimit();

    int getLocalLowLimit(TraversalParent parent, List<HasContainer> hasContainers);

    int getHighLimit();

    int getLocalHighLimit(TraversalParent parent, List<HasContainer> hasContainers);

    static boolean validJanusGraphHas(HasContainer has) {
        if (has.getPredicate() instanceof ConnectiveP) {
            final List<? extends P<?>> predicates = ((ConnectiveP<?>) has.getPredicate()).getPredicates();
            return predicates.stream().allMatch(p-> validJanusGraphHas(new HasContainer(has.getKey(), p)));
        } else {
            return JanusGraphPredicateUtils.supports(has.getBiPredicate());
        }
    }

    static boolean validJanusGraphHas(Iterable<HasContainer> has) {
        for (final HasContainer h : has) {
            if (!validJanusGraphHas(h)) return false;
        }
        return true;
    }

    static boolean validJanusGraphOrder(OrderGlobalStep orderGlobalStep, Traversal rootTraversal,
                                          boolean isVertexOrder) {
        final List<Pair<Traversal.Admin, Object>> comparators = orderGlobalStep.getComparators();
        for(final Pair<Traversal.Admin, Object> comp : comparators) {
            final String key;
            final Order order;
            if (comp.getValue0() instanceof ValueTraversal &&
                comp.getValue1() instanceof Order) {
                // a by(key) whose traversal yields a value where the key has none, as ProductiveByStrategy makes it,
                // keeps the elements without the key, which TinkerPop then orders itself
                if (((ValueTraversal) comp.getValue0()).getBypassTraversal() != null) return false;
                key = ((ValueTraversal) comp.getValue0()).getPropertyKey();
                order = (Order) comp.getValue1();
            } else if (comp.getValue0() instanceof IdentityTraversal
                && comp.getValue1() instanceof ElementValueComparator) {
                final ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                if (!(evc.getValueComparator() instanceof Order)) return false;
                key = evc.getPropertyKey();
                order = (Order) evc.getValueComparator();
            } else {
                // do not fold comparators that include nested traversals that are not simple ElementValues
                return false;
            }
            // shuffle marks a random order, which TinkerPop gives and no query sorts by
            if (order == Order.shuffle) return false;

            // check if all mixed index backends of the graph supports ordering for LIST cardinality properties
            // if this is the case, validJanusGraphOrder() method can return true not only for SINGLE
            // but also for LIST cardinality properties
            StandardJanusGraph graph = JanusGraphTraversalUtil.getJanusGraph(rootTraversal.asAdmin());
            boolean allMixedIndexBackendSupportsOrderingListProperty = graph.getIndexSerializer().allMixedIndexBackendSupportsOrderingListProperty();

            final JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(rootTraversal.asAdmin());
            final PropertyKey pKey = tx.getPropertyKey(key);
            if (pKey == null
                || !(Comparable.class.isAssignableFrom(pKey.dataType()))
                || (isVertexOrder && !allMixedIndexBackendSupportsOrderingListProperty && pKey.cardinality() != Cardinality.SINGLE)) {
                return false;
            }
        }
        return true;
    }

    /*
     * Whether an order() of an or() branch isn't folded in, and may leave out the element which TinkerPop runs the
     * branch for, as one by(key, shuffle) where the key has no value does: the or() is then TinkerPop's, as its branch
     * would be folded in without that order. An order whose by() modulators each yield a value for every element, one
     * of the element itself, its id or its label, or one which ProductiveByStrategy makes yield null where the key has
     * none, leaves out none, so the or() is folded in without it, as before.
     */
    static boolean dropsElementsUnfolded(final OrderGlobalStep<?, ?> order) {
        final Step<?, ?> start = order.getTraversal().getParent().asStep().getTraversal().getStartStep();
        final boolean isVertexOrder = !(start instanceof GraphStep) || ((GraphStep<?, ?>) start).returnsVertex();
        if (validJanusGraphOrder(order, TraversalHelper.getRootTraversal(order.getTraversal()), isVertexOrder)) {
            return false;
        }
        for (final Pair<? extends Traversal.Admin<?, ?>, ?> comparator : order.getComparators()) {
            if (!yieldsForEveryElement(comparator.getValue0())) {
                return true;
            }
        }
        return false;
    }

    //Whether a by() modulator yields a value for every element: see dropsElementsUnfolded
    static boolean yieldsForEveryElement(final Traversal.Admin<?, ?> modulator) {
        if (modulator instanceof IdentityTraversal) {
            return true;
        } else if (modulator instanceof org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal) {
            final org.apache.tinkerpop.gremlin.structure.T token =
                ((org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal<?, ?>) modulator).getToken();
            return token == org.apache.tinkerpop.gremlin.structure.T.id
                || token == org.apache.tinkerpop.gremlin.structure.T.label;
        } else if (modulator instanceof ValueTraversal) {
            //ProductiveByStrategy's bypass is a coalesce() of the value and a constant null
            final Traversal.Admin<?, ?> bypass = ((ValueTraversal<?, ?>) modulator).getBypassTraversal();
            if (bypass == null || !(bypass.getStartStep() instanceof CoalesceStep)) {
                return false;
            }
            final List<? extends Traversal.Admin<?, ?>> children =
                ((CoalesceStep<?, ?>) bypass.getStartStep()).getLocalChildren();
            final Traversal.Admin<?, ?> last = children.get(children.size() - 1);
            return last instanceof org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal
                && ((org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal<?, ?>) last).next()
                    == null;
        }
        return false;
    }

    /**
     * Whether one of the conditions is {@code has(key)} of the key, a not-equal to null as JanusGraph holds it, so
     * that an order by the key needs no such condition of its own; other conditions on the key, which leave out the
     * elements without a value as well, aren't recognised
     */
    static boolean requiresAValueOf(final String key, final List<HasContainer> conditions) {
        for (final HasContainer condition : conditions) {
            if (key.equals(condition.getKey()) && condition.getValue() == null
                    && JanusGraphPredicateUtils.convert(condition.getBiPredicate()) == Cmp.NOT_EQUAL) {
                return true;
            }
        }
        return false;
    }

    static void foldInIds(final HasStepFolder janusgraphStep, final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                final HasContainerHolder<?, ?> hasContainerHolder = (HasContainerHolder<?, ?>) currentStep;
                final GraphStep graphStep = (GraphStep) janusgraphStep;
                // HasContainer collection that we get back is immutable so we keep track of which containers
                // need to be deleted after they've been folded into the JanusGraphStep and then remove them from their
                // step using HasContainer.removeHasContainer
                final List<HasContainer> removableHasContainers = new ArrayList<>();
                final Set<String> stepLabels = currentStep.getLabels();
                hasContainerHolder.getHasContainers().forEach(hasContainer -> {
                    if (GraphStep.processHasContainerIds(graphStep, hasContainer)) {
                        stepLabels.forEach(janusgraphStep::addLabel);
                        // this has container is no longer needed because its ids will be folded into the JanusGraphStep
                        removableHasContainers.add(hasContainer);
                    }
                });

                if (!removableHasContainers.isEmpty()) {
                    removableHasContainers.forEach(hasContainerHolder::removeHasContainer);
                }
                // if all has containers have been removed, the current step can be removed
                if (hasContainerHolder.getHasContainers().isEmpty()) {
                    traversal.removeStep(currentStep);
                }
            }
            else if (currentStep instanceof IdentityStep || currentStep instanceof NoOpBarrierStep) {
                // do nothing, has no impact
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static void foldInHasContainer(final HasStepFolder janusgraphStep, final Traversal.Admin<?, ?> traversal,
            final Traversal<?, ?> rootTraversal) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof OrStep && janusgraphStep instanceof JanusGraphStep) {
                for (final Traversal.Admin<?, ?> child : ((OrStep<?>) currentStep).getLocalChildren()) {
                    if (!validFoldInHasContainer(child.getStartStep(), false)){
                        return;
                    }
                }
                ((OrStep<?>) currentStep).getLocalChildren().forEach(t ->localFoldInHasContainer(janusgraphStep, t.getStartStep(), t, rootTraversal));
                currentStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(currentStep);
            } else if (currentStep instanceof HasContainerHolder && validFoldInHasContainer(currentStep, true)){
                List<HasContainer> hasContainersList = ((HasContainerHolder<?, ?>) currentStep).getHasContainers();
                janusgraphStep.ensureAdditionalHasContainersCapacity(hasContainersList.size());
                for(HasContainer hasContainer : hasContainersList){
                    janusgraphStep.addHasContainer(JanusGraphPredicateUtils.convert(hasContainer));
                }
                currentStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(currentStep);
            } else if (isExistsStep(currentStep)) {
                currentStep = foldInHasFilter(traversal, (TraversalFilterStep) currentStep);
                continue;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static void localFoldInHasContainer(final HasStepFolder janusgraphStep, final Step<?, ?>  tinkerpopStep, final Traversal.Admin<?, ?> traversal,
            final Traversal<?, ?> rootTraversal){
        Step<?, ?> currentStep = tinkerpopStep;
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                List<HasContainer> localHasContainers = janusgraphStep.addLocalHasContainersConvertingAndPContainers(
                    traversal.getParent(), ((HasContainerHolder<?, ?>) currentStep).getHasContainers());
                currentStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(currentStep);
                currentStep = foldInOrder(janusgraphStep, currentStep, traversal, rootTraversal, janusgraphStep instanceof JanusGraphStep && ((JanusGraphStep)janusgraphStep).returnsVertex(), localHasContainers);
                foldInRange(janusgraphStep, currentStep, traversal, localHasContainers);
            } else if (isExistsStep(currentStep)) {
                currentStep = foldInHasFilter(traversal, (TraversalFilterStep) currentStep);
                continue;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static boolean validFoldInHasContainer(final Step<?, ?>  tinkerpopStep, final boolean defaultValue){
        Step<?, ?> currentStep = tinkerpopStep;
        Boolean toReturn = null;
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof HasContainerHolder) {
                final Iterable<HasContainer> containers = ((HasContainerHolder<?, ?>) currentStep).getHasContainers();
                toReturn = toReturn == null ? validJanusGraphHas(containers) : toReturn && validJanusGraphHas(containers);
            } else if (isExistsStep(currentStep)) {
                toReturn = true;
            } else if (currentStep instanceof OrderGlobalStep && !defaultValue
                    && dropsElementsUnfolded((OrderGlobalStep<?, ?>) currentStep)) {
                toReturn = false;
                break;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep) && !(currentStep instanceof RangeGlobalStep) && !(currentStep instanceof OrderGlobalStep)) {
                toReturn = toReturn != null && (toReturn && defaultValue);
                break;
            }
            currentStep = currentStep.getNextStep();
        }
        return Boolean.TRUE.equals(toReturn);
    }

    /**
     * Check if a given step is an "exists" step, i.e. has(key) step
     * Tinkerpop translates has(key) gremlin query into TraversalFilterStep(PropertiesStep)
     * @param step
     * @return true if given step is essentially an "exists" step
     */
    static boolean isExistsStep(final Step<?, ?> step) {
        if (step instanceof TraversalFilterStep) {
            List<Traversal.Admin> traversals = ((TraversalFilterStep) step).getLocalChildren();
            assert traversals.size() == 1;
            List<Step> steps = traversals.get(0).getSteps();
            if (!(steps.size() == 1 && steps.get(0) instanceof PropertiesStep)) return false;
            return ((PropertiesStep) steps.get(0)).getPropertyKeys().length == 1;
        }
        return false;
    }

    /**
     * Convert a TraversalFilterStep that is essentially "has(key)" into a normal HasStep
     * @param traversal local traversal
     * @param currentStep
     */
    static Step foldInHasFilter(final Traversal.Admin<?, ?> traversal, final TraversalFilterStep<?> currentStep) {
        List<? extends Traversal.Admin<?, ?>> traversals = currentStep.getLocalChildren();
        assert(traversals.size() == 1);
        List<Step> steps = traversals.get(0).getSteps();
        assert(steps.size() == 1 && steps.get(0) instanceof PropertiesStep);
        String[] propertyKeys = ((PropertiesStep) steps.get(0)).getPropertyKeys();
        assert(propertyKeys.length == 1);
        HasStep hasStep = new HasStep(traversal, new HasContainer(propertyKeys[0], P.neq(null)));
        currentStep.getLabels().forEach(hasStep::addLabel);
        TraversalHelper.replaceStep(currentStep, hasStep, traversal);
        return hasStep;
    }

    static Step<?, ?> foldInOrder(final HasStepFolder janusgraphStep, final Step<?, ?>  tinkerpopStep, final Traversal.Admin<?, ?> traversal,
                                   final Traversal<?, ?> rootTraversal, boolean isVertexOrder, final List<HasContainer> hasContainers) {
        Step<?, ?> currentStep = tinkerpopStep;
        final List<OrderGlobalStep<?, ?>> orderSteps = new ArrayList<>();
        while (true) {
            if (currentStep instanceof OrderGlobalStep) {
                orderSteps.add((OrderGlobalStep<?, ?>) currentStep);
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof HasStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        //TinkerPop sorts by each order() in turn, keeping the order of ties, and each leaves out the elements for which
        //one of its by() modulators has no value: so the elements come in the order of the last one, and where that
        //ties, of those before it. These orders are folded in together, the last one's keys first, or none of them
        final List<OrderEntry> orders = new ArrayList<>();
        for (int i = orderSteps.size() - 1; i >= 0; i--) {
            if (!validJanusGraphOrder(orderSteps.get(i), rootTraversal, isVertexOrder)) {
                return currentStep;
            }
            final List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Object>>> comparators =
                (List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Object>>>)
                    ((OrderGlobalStep) orderSteps.get(i)).getComparators();
            for (final Pair<Traversal.Admin<Object, Comparable>, Comparator<Object>> comp : comparators) {
                final String key;
                final Order order;
                if (comp.getValue0() instanceof ValueTraversal) {
                    final ValueTraversal evt = (ValueTraversal) comp.getValue0();
                    key = evt.getPropertyKey();
                    order = (Order) comp.getValue1();
                } else {
                    final ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                    key = evc.getPropertyKey();
                    order = (Order) evc.getValueComparator();
                }
                //a key which the orders taken so far already sort by, those of later order() steps or of earlier
                //by() modulators of this one, doesn't change the order
                if (orders.stream().noneMatch(entry -> entry.key.equals(key))) {
                    orders.add(new OrderEntry(key, order));
                }
            }
        }
        //A query of the edges or properties of a vertex takes a single order key
        if (orders.isEmpty() || orders.size() > 1 && !(janusgraphStep instanceof JanusGraphStep)) {
            return currentStep;
        }
        //Add orders to HasStepFolder
        for (final OrderEntry order : orders) {
            if (hasContainers == null) {
                janusgraphStep.orderBy(order.key, order.order);
            } else {
                janusgraphStep.localOrderBy(traversal.getParent(), hasContainers, order.key, order.order);
            }
        }
        for (final OrderGlobalStep<?, ?> orderStep : orderSteps) {
            orderStep.getLabels().forEach(janusgraphStep::addLabel);
            traversal.removeStep(orderStep);
        }
        return currentStep;
    }

    static void splitAndP(final List<HasContainer> hasContainers, final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                hasContainers.add(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else {
            hasContainers.add(hasContainer);
        }
    }

    class OrderEntry {

        public final String key;
        public final Order order;

        public OrderEntry(String key, Order order) {
            this.key = key;
            this.order = order;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final OrderEntry that = (OrderEntry) o;

            if (!Objects.equals(key, that.key)) return false;
            return order == that.order;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (order != null ? order.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "OrderEntry{" +
                "key='" + key + '\'' +
                ", order=" + order +
                '}';
        }
    }

    static void foldInRange(final HasStepFolder janusgraphStep, final Step<?, ?>  tinkerpopStep, final Traversal.Admin<?, ?> traversal, final List<HasContainer> hasContainers) {
        final Step<?, ?> nextStep = tinkerpopStep instanceof IdentityStep ? JanusGraphTraversalUtil.getNextNonIdentityStep(tinkerpopStep): tinkerpopStep;
        if (nextStep instanceof RangeGlobalStep) {
            final RangeGlobalStep range = (RangeGlobalStep) nextStep;
            int low = 0;
            if (janusgraphStep instanceof JanusGraphStep) {
                low = QueryUtil.convertLimit(range.getLowRange());
                low = QueryUtil.mergeLowLimits(low, hasContainers == null ? janusgraphStep.getLowLimit(): janusgraphStep.getLocalLowLimit(traversal.getParent(), hasContainers));
            }
            int high = QueryUtil.convertLimit(range.getHighRange());
            high = QueryUtil.mergeHighLimits(high, hasContainers == null ? janusgraphStep.getHighLimit(): janusgraphStep.getLocalHighLimit(traversal.getParent(), hasContainers));
            if (hasContainers == null) {
                janusgraphStep.setLimit(low, high);
            } else {
                janusgraphStep.setLocalLimit(traversal.getParent(), hasContainers, low, high);
            }
            if (janusgraphStep instanceof JanusGraphStep || range.getLowRange() == 0) { //Range can be removed since there is JanusGraphStep or no offset
                nextStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(nextStep);
            }
        }
    }

    /**
     * @param janusgraphStep The step to test
     * @return True if there are 'has' steps following this step and no subsequent range limit step
     */
    static boolean foldableHasContainerNoLimit(FlatMapStep<?, ?> janusgraphStep) {
        boolean foldableHasContainerNoLimit = false;
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof OrStep) {
                for (final Traversal.Admin<?, ?> child : ((OrStep<?>) currentStep).getLocalChildren()) {
                    if (!validFoldInHasContainer(child.getStartStep(), false)){
                        return false;
                    }
                }
                foldableHasContainerNoLimit =  true;
            } else if (currentStep instanceof HasContainerHolder) {
                if  (validFoldInHasContainer(currentStep, true)) {
                    foldableHasContainerNoLimit =  true;
                }
            } else if (currentStep instanceof RangeGlobalStep) {
                return false;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        return foldableHasContainerNoLimit;
    }
}
