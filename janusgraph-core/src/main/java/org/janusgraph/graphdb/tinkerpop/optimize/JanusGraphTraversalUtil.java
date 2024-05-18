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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.computer.FulgoraElementTraversal;
import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable;
import org.janusgraph.graphdb.tinkerpop.optimize.step.NoOpBarrierVertexOnlyStep;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphTraversalUtil {

    /**
     * These parent steps can benefit from a JanusGraphMultiQueryStep capturing the parent's starts and
     * using them to initialise a JanusGraphVertexStep if it's the first step of any child traversal.
     * <br>
     * Only steps which proxy traversers directly to all child traversal starts are eligible. Steps which
     * don't proxy traversers to ALL child traversals are not eligible. Steps which are using different starts
     * depending on `loops` count (like `repeat` step) are not eligible either.<br>
     * Including steps which don't accept child traversals here is OK, but redundant.
     * Including non-eligible steps into this list won't affect query result correctness, but may influence
     * query performance due to wrong elements being queried in batch requests instead of expected elements.
     * Below are examples which may help to determine if the step is eligible or not.<br>
     * <br>
     * `g.V().or(has("name", "foo"), has("age", 123))`
     * `or` step is eligible because it passes its incoming vertices into child traversals
     * (`has("name", "foo")` and `has("age", 123)`).
     * <br>
     * `g.V().where(out("knows"))`
     * `where` step is eligible step because it passes its
     * incoming vertices into the child traversal (`out("knows")`).
     * <br>
     * `g.V(123).repeat(out("knows")).emit()`
     * `repeat` step is NOT eligible step. Even so it's first iteration
     * passes incoming elements to repeatTraversal, the subsequent iterations don't (when loops > 0).
     * <br>
     * `g.V().match(as('a').out('created').has('name', 'lop').as('b'),
     *              as('b').in('created').has('age', 29).as('c')).
     *        select('a','c').by('name')`
     * `match` step is NOT eligible step. Even so it passes incoming elements
     * to one of the child traversal it doesn't do so for another traversal.
     */
    private static final List<Class<? extends TraversalParent>> MULTIQUERY_COMPATIBLE_PARENTS =
        Arrays.asList(
            ConnectiveStep.class, // AndStep, OrStep
            BranchStep.class, // ChooseStep, UnionStep
            CallStep.class,
            CoalesceStep.class,
            GroupStep.class,
            GroupSideEffectStep.class,
            LocalStep.class,
            NotStep.class,
            OptionalStep.class,
            OrderGlobalStep.class,
            OrderLocalStep.class,
            ProjectStep.class,
            PropertyMapStep.class,
            TraversalFilterStep.class,
            TraversalMapStep.class,
            TraversalSideEffectStep.class,
            WherePredicateStep.class,
            WhereTraversalStep.class,
            AggregateLocalStep.class,
            AggregateGlobalStep.class,
            GroupCountStep.class,
            TraversalFlatMapStep.class,
            TraversalMapStep.class
        );

    public static StandardJanusGraph getJanusGraph(final Traversal.Admin<?, ?> traversal) {
        Optional<Graph> optionalGraph = traversal.getGraph();
        if (!optionalGraph.isPresent()) {
            return null;
        }
        Graph graph = optionalGraph.get();
        if (graph instanceof StandardJanusGraph) {
            return (StandardJanusGraph) graph;
        }
        if (graph instanceof StandardJanusGraphTx) {
            return ((StandardJanusGraphTx) graph).getGraph();
        }
        return null;
    }

    public static Optional<StandardJanusGraphTx> getJanusGraphTx(final Traversal.Admin<?, ?> traversal) {
        Optional<Graph> optionalGraph = traversal.getGraph();
        if (!optionalGraph.isPresent()) {
            return Optional.empty();
        }
        Graph graph = optionalGraph.get();
        if (graph instanceof StandardJanusGraphTx) {
            return Optional.of((StandardJanusGraphTx) graph);
        }
        return Optional.empty();
    }

    public static JanusGraphVertex getJanusGraphVertex(Element v) {
        while (v instanceof WrappedVertex) {
            v = ((WrappedVertex<Vertex>) v).getBaseVertex();
        }
        if (v instanceof JanusGraphVertex) {
            return (JanusGraphVertex) v;
        } else throw new IllegalArgumentException("Expected traverser of JanusGraph vertex but found: " + v);
    }

    public static JanusGraphVertex getJanusGraphVertex(Traverser<? extends Element> traverser) {
        return getJanusGraphVertex(traverser.get());
    }

    public static boolean isEdgeReturnStep(JanusGraphVertexStep vertexStep) {
        return Edge.class.isAssignableFrom(vertexStep.getReturnClass());
    }

    public static boolean isVertexReturnStep(JanusGraphVertexStep vertexStep) {
        return Vertex.class.isAssignableFrom(vertexStep.getReturnClass());
    }

    public static Step getNextNonIdentityStep(final Step start) {
        Step currentStep = start.getNextStep();
        //Skip over identity steps
        while (currentStep instanceof IdentityStep) currentStep = currentStep.getNextStep();
        return currentStep;
    }

    public static JanusGraphTransaction getTx(Traversal.Admin<?, ?> traversal) {
        final JanusGraphTransaction tx;
        Optional<Graph> optGraph = TraversalHelper.getRootTraversal(traversal.asAdmin()).getGraph();

        if (traversal instanceof FulgoraElementTraversal) {
            tx = (JanusGraphTransaction) optGraph.get();
        } else {
            if (!optGraph.isPresent())
                throw new IllegalArgumentException("Traversal is not bound to a graph: " + traversal);
            Graph graph = optGraph.get();
            if (graph instanceof JanusGraphTransaction) tx = (JanusGraphTransaction) graph;
            else if (graph instanceof JanusGraphBlueprintsGraph) tx = ((JanusGraphBlueprintsGraph) graph).getCurrentThreadTx();
            else throw new IllegalArgumentException("Traversal is not bound to a JanusGraph Graph, but: " + graph);
        }
        if (tx == null)
            throw new IllegalArgumentException("Not a valid start step for a JanusGraph traversal: " + traversal);
        if (tx.isOpen()) return tx;
        else return ((StandardJanusGraphTx) tx).getNextTx();
    }

    /**
     * In case `step` can be considered as eligible start step then
     * this method returns the most outer eligible parent step.
     * The most outer eligible parent for the provided `step` is
     * the one which is directly or indirectly includes the provided `step`
     * and satisfies the following criteria:
     * <br>
     * - It includes the maximum number of nested eligible parents for the provided `step`.
     * - All the nested parents, including the provided `step` must be considered as `start` steps
     * of their respective traversals.
     * - Each such nested parent of the provided `step` must be parentToChildProxyStep (determined by
     * `isParentToChildProxyStep(nextStep)` method).
     * <br>
     * In case `step` don't have any eligible parents then the provided `step` will be returned.
     */
    public static Step<?,?> findMostOuterEligibleStart(Step<?,?> step){
        Step<?,?> resultStep = step;
        Step<?,?> nextStep = step.getTraversal().getParent().asStep();
        while (!(nextStep instanceof EmptyStep) && isMultiQueryCompatibleParent(nextStep)){
            resultStep = nextStep;
            if(!isStartStep(nextStep)){
                break;
            }
            nextStep = nextStep.getTraversal().getParent().asStep();
        }
        return resultStep;
    }

    public static boolean isMultiQueryCompatibleParent(Step<?,?> step){
        for(Class<?> allowedParentClass : MULTIQUERY_COMPATIBLE_PARENTS){
            if (allowedParentClass.isInstance(step)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Backtraces the traversal for the position where a MultiQueriable step would expect its corresponding
     * JanusGraphMultiQueryStep(s). In case of MultiQueriables nested in RepeatSteps, multiple destinations
     * are returned.
     * @param multiQueriable The MultiQuery compatible step whose MultiQueryStep positions shall be searched.
     * @return The step before which the MultiQueryStep is located or expected.
     */
    public static MultiQueryPositions getAllMultiQueryPositionsForMultiQueriable(final MultiQueriable<?, ?> multiQueriable,
                                                                                 final boolean multiNestedRepeatEligible,
                                                                                 final boolean multiNestedRepeatNextIterationEligible) {
        MultiQueryPositions multiQueryPositions = new MultiQueryPositions();

        if(!isStartStep(multiQueriable)){
            getLocalMultiQueryPositionForStep(multiQueriable)
                .ifPresent(step -> multiQueryPositions.currentLoopMultiQueryStepLocations.add(step));
            return multiQueryPositions;
        }

        Step<?,?> currentStep = multiQueriable;
        boolean parentRepeatIsUsed = false;

        do {
            Step<?,?> previousStep = currentStep;
            currentStep = findMostOuterEligibleStart(currentStep);
            Step<?,?> parentStep = currentStep.getTraversal().getParent().asStep();
            if(!(parentStep instanceof RepeatStep) || currentStep!=previousStep && !isStartStep(currentStep)){
                if(!parentRepeatIsUsed){
                    getLocalMultiQueryPositionForStep(currentStep)
                        .ifPresent(step -> multiQueryPositions.currentLoopMultiQueryStepLocations.add(step));
                }
                return multiQueryPositions;
            }

            final RepeatStep<?> parentRepeatStep = (RepeatStep<?>) parentStep;
            if(!parentRepeatIsUsed){
                getEndMultiQueryPosition(parentRepeatStep.getRepeatTraversal())
                    .ifPresent(step -> multiQueryPositions.nextLoopMultiQueryStepLocation = step);
                parentRepeatIsUsed = true;
            } else if(multiNestedRepeatNextIterationEligible) {
                getEndMultiQueryPosition(parentRepeatStep.getRepeatTraversal())
                    .ifPresent(step -> multiQueryPositions.firstLoopMultiQueryStepLocations.add(step));
            }
            Traversal.Admin<?,?> traversal = currentStep.getTraversal();
            if(traversal == parentRepeatStep.getRepeatTraversal() ||
                parentRepeatStep.emitFirst && traversal == parentRepeatStep.getEmitTraversal() ||
                parentRepeatStep.untilFirst && traversal == parentRepeatStep.getUntilTraversal()){

                if(isStartStep(parentRepeatStep)){
                    currentStep = findMostOuterEligibleStart(parentRepeatStep);
                    getLocalMultiQueryPositionForStep(currentStep)
                        .ifPresent(step -> multiQueryPositions.firstLoopMultiQueryStepLocations.add(step));
                    if(!multiNestedRepeatEligible){
                        return multiQueryPositions;
                    }
                } else {
                    getLocalMultiQueryPositionForStep(parentRepeatStep)
                        .ifPresent(step -> multiQueryPositions.firstLoopMultiQueryStepLocations.add(step));
                    return multiQueryPositions;
                }

            } else {
                return multiQueryPositions;
            }

        } while (true);
    }

    /**
     * Returns multi query position for the end of the traversal.
     * This can be useful in case by the end of the traversal, vertices should be registered for prefetching
     * in some other places.
     */
    public static Optional<Step> getEndMultiQueryPosition(Traversal.Admin repeatTraversal){
        return repeatTraversal == null ? Optional.empty() : getLocalMultiQueryPositionForStep(repeatTraversal.getEndStep());
    }

    /**
     * For a MultiQuery compatible step, this method searches the correct position in the step's traversal at which
     * a <code>JanusGraphMultiQueryStep</code> should be inserted. Only the traversal of the given step is considered,
     * parent and child traversals are not taken into account.
     * @param step The MultiQuery compatible step.
     * @return The step before which a <code>JanusGraphMultiQueryStep</code> should be inserted.
     * @see org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep
     */
    public static Optional<Step> getLocalMultiQueryPositionForStep(Step<?, ?> step) {
        Step currentStep = step;
        Step previousStep = step.getPreviousStep();
        while (previousStep instanceof SideEffectStep || previousStep instanceof ProfileStep) {
            currentStep = previousStep;
            previousStep = previousStep.getPreviousStep();
        }
        if(previousStep instanceof NoOpBarrierStep || previousStep instanceof NoOpBarrierVertexOnlyStep){
            return Optional.of(previousStep);
        }
        if(currentStep instanceof EmptyStep || currentStep instanceof StartStep){
            return Optional.empty();
        }
        return Optional.of(currentStep);
    }

    /**
     * Returns local barrier step size if that barrier step is not generated by JanusGraphMultiQueryStep.
     */
    public static Optional<Integer> getLocalNonMultiQueryProvidedBatchSize(MultiQueriable<?, ?> multiQueriable) {
        Optional<Step> optionalPosition = getLocalMultiQueryPositionForStep(multiQueriable);
        if(!optionalPosition.isPresent()){
            return Optional.empty();
        }
        Step<?,?> step = optionalPosition.get();
        if(!(step instanceof NoOpBarrierStep || step instanceof NoOpBarrierVertexOnlyStep)){
            return Optional.empty();
        }
        Step<?,?> previousStep = step.getPreviousStep();
        if(previousStep instanceof JanusGraphMultiQueryStep && ((JanusGraphMultiQueryStep) previousStep).getGeneratedBarrierStep() == step){
            return Optional.empty();
        }
        if(step instanceof NoOpBarrierStep){
            return Optional.of(((NoOpBarrierStep) step).getMaxBarrierSize());
        }
        return Optional.of(((NoOpBarrierVertexOnlyStep) step).getMaxBarrierSize());
    }

    /**
     * Checks if the provided step is the start step of its Traversal skipping any `IdentityStep`, `NoOpBarrierStep`,
     * `SideEffectStep`, `ProfileStep`, and `JanusGraphMultiQueryStep` steps at the beginning.
     * @param step Step to check.
     * @return `true` if the step can be considered as the start of its traversal. `false` otherwise.
     */
    public static boolean isStartStep(Step<?, ?> step){
        Step<?,?> startStep = step;
        do {
            startStep = startStep.getPreviousStep();
        } while (startStep instanceof JanusGraphMultiQueryStep || startStep instanceof NoOpBarrierStep || startStep instanceof NoOpBarrierVertexOnlyStep ||
            startStep instanceof IdentityStep || startStep instanceof SideEffectStep || startStep instanceof ProfileStep);
        return startStep instanceof EmptyStep || startStep instanceof StartStep;
    }

    /**
     * This method closely matches the behavior implemented in <code>LazyBarrierStrategy</code> which ensures that
     * no <code>NoOpBarrierStep</code>s are inserted if path labels are required. Since the same limitation applies
     * to <code>JanusGraphMultiQueryStep</code>s, the definition of legal positions for this step is the same.
     * @param step The step which follows the JanusGraphMultiQueryStep to be inserted.
     * @return <code>true</code> if no path labels are required for this position, otherwise <code>false</code>.
     * @see org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy
     */
    public static boolean isLegalMultiQueryPosition(Step<?, ?> step) {
        if (step.getTraversal().getTraverserRequirements().contains(TraverserRequirement.PATH)) return false;
        boolean labeledPath = false;
        Step currentStep = step.getTraversal().getStartStep();
        while (!currentStep.equals(step, true)) {
            if (step instanceof PathProcessor) {
                final Set<String> keepLabels = ((PathProcessor) step).getKeepLabels();
                labeledPath &= keepLabels == null || !keepLabels.isEmpty();
            }
            labeledPath |= !step.getLabels().isEmpty();
            currentStep = currentStep.getNextStep();
        }
        return !labeledPath;
    }

    /**
     * Starting at the given step, this method searches the traversal backwards to find the most recent step which
     * matches the given class. Only the traversal of the given step is scanned and parent or child traversals are
     * not taken into account.
     * @param stepClass The class of the requested step.
     * @param start The step from which the search is started.
     * @param <S> The class of the requested step.
     * @return An Optional which contains the requested step if it was found.
     */
    public static <S> Optional<S> getPreviousStepOfClass(final Class<S> stepClass, Step<?,?> start) {
        Step currentStep = start;
        while (currentStep != null && !currentStep.getClass().equals(stepClass) && !(currentStep instanceof EmptyStep)) {
            currentStep = currentStep.getPreviousStep();
        }
        return currentStep != null && currentStep.getClass().equals(stepClass) ? Optional.of((S) currentStep) : Optional.empty();
    }

    /**
     * Returns a list of steps from the traversal, which match a given predicate.
     * @param predicate Whether or not a step should be in the returned list.
     * @param traversal The traversal whose steps should be used.
     * @return The list of matching steps.
     */
    public static List<Step> getSteps(Predicate<Step> predicate, Traversal.Admin<?,?> traversal) {
        return traversal.getSteps().stream().filter(predicate).collect(Collectors.toList());
    }
}
