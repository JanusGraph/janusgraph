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

import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.computer.FulgoraElementTraversal;
import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
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
     */
    private static final List<Class<? extends TraversalParent>> MULTIQUERY_COMPATIBLE_PARENTS =
            Arrays.asList(BranchStep.class, OptionalStep.class, RepeatStep.class, TraversalFilterStep.class);

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
     * Backtraces the traversal for the position where a MultiQueriable step would expect its corresponding
     * JanusGraphMultiQueryStep(s). In case of MultiQueriables nested in RepeatSteps, multiple destinations
     * are returned.
     * @param multiQueriable The MultiQuery compatible step whose MultiQueryStep positions shall be searched.
     * @return The step before which the MultiQueryStep is located or expected.
     */
    public static List<Step> getAllMultiQueryPositionsForMultiQueriable(Step<?, ?> multiQueriable) {
        List<Step> multiQueryStepLocations = new ArrayList<>();
        Queue<Step> rawLocations = new LinkedList<>();
        Step currentStep = multiQueriable;

        do {
            rawLocations.add(currentStep);
            currentStep = currentStep.getTraversal().getParent().asStep();
        } while (currentStep instanceof RepeatStep);

        while (!rawLocations.isEmpty()) {
            currentStep = rawLocations.poll();
            Optional<Step> positionInLocalTraversal = getLocalMultiQueryPositionForStep(currentStep);
            if (positionInLocalTraversal.isPresent()) {
                multiQueryStepLocations.add(positionInLocalTraversal.get());
            } else {
                rawLocations.add(currentStep.getTraversal().getParent().asStep());
            }
        }

        return multiQueryStepLocations;
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
        if (previousStep instanceof EmptyStep || previousStep instanceof StartStep) {
            final Step parentStep = step.getTraversal().getParent().asStep();
            if (!(parentStep instanceof RepeatStep) && isMultiQueryCompatibleParent(parentStep)) {
                return Optional.empty(); // no position found for JanusGraphMultiQueryStep in this local traversal
            } else {
                return Optional.of(currentStep); // place JanusGraphMultiQueryStep at the stat of the local traversal
            }
        } else if (previousStep instanceof NoOpBarrierStep) {
            return Optional.of(previousStep);
        } else {
            return Optional.of(currentStep);
        }
    }

    /**
     * Checks whether this step is a traversal parent for which a preceding <code>JanusGraphMultiQueryStep</code>
     * can have a positive impact on the step's child traversals.
     * @param step The step to be checked.
     * @return <code>true</code> if the step's child traversals can possibly benefit of a preceding
     *         <code>JanusGraphMultiQueryStep</code>, otherwise <code>false</code>.
     */
    public static boolean isMultiQueryCompatibleParent(Step<?, ?> step) {
        for (Class<? extends TraversalParent> c : MULTIQUERY_COMPATIBLE_PARENTS) {
            if (c.isInstance(step)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether a step can profit of a preceding <code>JanusGraphMultiQueryStep</code>.
     * @param step The step for which the condition is checked.
     * @return <code>true</code> if the step is either a <code>MultiQueriable</code> or is a MultiQuery compatible
     *         parent step.
     * @see MultiQueriable
     */
    public static boolean isMultiQueryCompatibleStep(Step<?, ?> step) {
        return step instanceof MultiQueriable || isMultiQueryCompatibleParent(step);
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
