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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Ranging;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface HasStepFolder<S, E> extends Step<S, E> {

    void addAll(Iterable<HasContainer> hasContainers);

    void orderBy(String key, Order order);

    void setLimit(int limit);

    int getLimit();

    static boolean validJanusGraphHas(HasContainer has) {
        if (has.getPredicate() instanceof AndP) {
            final List<? extends P<?>> predicates = ((AndP<?>) has.getPredicate()).getPredicates();
            return predicates.stream().allMatch(p-> validJanusGraphHas(new HasContainer(has.getKey(), p)));
        } else {
            return JanusGraphPredicate.Converter.supports(has.getBiPredicate());
        }
    }

    static boolean validJanusGraphHas(Iterable<HasContainer> has) {
        for (HasContainer h : has) {
            if (!validJanusGraphHas(h)) return false;
        }
        return true;
    }

    static boolean validJanusGraphOrder(OrderGlobalStep orderGlobalStep, Traversal rootTraversal,
                                          boolean isVertexOrder) {
        for (Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>> comp : (List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>>>) orderGlobalStep.getComparators()) {
            if (!(comp.getValue1() instanceof ElementValueComparator)) return false;
            ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
            if (!(evc.getValueComparator() instanceof Order)) return false;

            JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(rootTraversal.asAdmin());
            String key = evc.getPropertyKey();
            PropertyKey propertyKey = tx.getPropertyKey(key);
            if (propertyKey == null || !(Comparable.class.isAssignableFrom(propertyKey.dataType()))) return false;
            if (isVertexOrder && propertyKey.cardinality() != Cardinality.SINGLE) return false;
        }
        return true;
    }

    static void foldInIds(final HasStepFolder janusgraphStep, final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                final HasContainerHolder hasContainerHolder = (HasContainerHolder) currentStep;
                final Set<Object> ids = new HashSet<>();
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
                        if (!ids.isEmpty()) {
                            // intersect ids (shouldn't this be handled in TP GraphStep.processHasContainerIds?)
                            ids.stream().filter(id -> Arrays.stream(graphStep.getIds()).noneMatch(id::equals))
                                .collect(Collectors.toSet()).forEach(ids::remove);
                            if (ids.isEmpty()) {
                                return;
                            }
                        } else {
                            ids.addAll(Arrays.asList(graphStep.getIds()));
                        }
                    }
                    // clear ids to allow folding in ids from next HasContainer if relevant
                    graphStep.clearIds();
                });
                graphStep.addIds(ids);
                if (!removableHasContainers.isEmpty()) {
                    removableHasContainers.forEach(hasContainerHolder::removeHasContainer);
                }
                // if all has containers have been removed, the current step can be removed
                if (hasContainerHolder.getHasContainers().isEmpty()) {
                    traversal.removeStep(currentStep);
                }
            }
            else if (currentStep instanceof IdentityStep) {
                // do nothing, has no impact
            } else if (currentStep instanceof NoOpBarrierStep) {
                // do nothing, has no impact
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static void foldInHasContainer(final HasStepFolder janusgraphStep, final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers();
                if (validJanusGraphHas(containers)) {
                    janusgraphStep.addAll(containers);
                    currentStep.getLabels().forEach(janusgraphStep::addLabel);
                    traversal.removeStep(currentStep);
                }
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static void foldInOrder(final HasStepFolder janusgraphStep, final Traversal.Admin<?, ?> traversal,
                                   final Traversal<?, ?> rootTraversal, boolean isVertexOrder) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        OrderGlobalStep<?, ?> lastOrder = null;
        while (true) {
            if (currentStep instanceof OrderGlobalStep) {
                if (lastOrder != null) { //Previous orders are rendered irrelevant by next order (since re-ordered)
                    lastOrder.getLabels().forEach(janusgraphStep::addLabel);
                    traversal.removeStep(lastOrder);
                }
                lastOrder = (OrderGlobalStep) currentStep;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof HasStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        if (lastOrder != null) {
            if (validJanusGraphOrder(lastOrder, rootTraversal, isVertexOrder)) {
                //Add orders to HasStepFolder
                for (Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>> comp : (List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>>>) ((OrderGlobalStep) lastOrder).getComparators()) {
                    ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                    janusgraphStep.orderBy(evc.getPropertyKey(), (Order) evc.getValueComparator());
                }
                lastOrder.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(lastOrder);
            }
        }
    }

    static void splitAndP(final List<HasContainer> hasContainers, final Iterable<HasContainer> has) {
        has.forEach(hasContainer -> {
            if (hasContainer.getPredicate() instanceof AndP) {
                for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                    hasContainers.add(new HasContainer(hasContainer.getKey(), predicate));
                }
            } else
                hasContainers.add(hasContainer);
        });
    }

    class OrderEntry {

        public final String key;
        public final Order order;

        public OrderEntry(String key, Order order) {
            this.key = key;
            this.order = order;
        }
    }

    static <E extends Ranging> void foldInRange(final HasStepFolder janusgraphStep, final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> nextStep = JanusGraphTraversalUtil.getNextNonIdentityStep(janusgraphStep);

        if (nextStep instanceof RangeGlobalStep) {
            RangeGlobalStep range = (RangeGlobalStep) nextStep;
            int limit = QueryUtil.convertLimit(range.getHighRange());
            janusgraphStep.setLimit(QueryUtil.mergeLimits(limit, janusgraphStep.getLimit()));
            if (range.getLowRange() == 0) { //Range can be removed since there is no offset
                nextStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(nextStep);
            }
        }
    }


}
