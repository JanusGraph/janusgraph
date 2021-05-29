// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public abstract class AdjacentVertexOptimizerStrategy<T extends FilterStep<?>>
    extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

    protected enum OptimizablePosition {
        NONE,
        V2V_ID,    // vertex-to-vertex step with following filter on id
        V2E_E2V_ID // vertex-to-edge step with following edge-to-vertex-step and filter on id
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        Set<Class<? extends ProviderOptimizationStrategy>> postStrategies = new HashSet<>();
        postStrategies.add(JanusGraphLocalQueryOptimizerStrategy.class);
        return postStrategies;
    }

    protected void optimizeStep(T step) {
        OptimizablePosition pos = getOptimizablePosition(step);
        if (pos != OptimizablePosition.NONE && isValidStep(step)) {
            replaceSequence(step, pos);
        }
    }

    protected abstract boolean isValidStep(T step);

    private OptimizablePosition getOptimizablePosition(T originalStep) {
        Step<?, ?> predecessor = originalStep.getPreviousStep();

        // ignore in-between LazyBarrierSteps
        if (predecessor instanceof NoOpBarrierStep) {
            predecessor = predecessor.getPreviousStep();
        }

        // match preceding out(), in() or both() steps
        if (predecessor instanceof VertexStep<?>) {
            if (((VertexStep<?>) predecessor).returnsVertex()) {
                return OptimizablePosition.V2V_ID;
            }
            return OptimizablePosition.NONE;
        }

        Step<?, ?> prePredecessor = predecessor.getPreviousStep();

        // ignore in-between LazyBarrierSteps
        if (prePredecessor instanceof NoOpBarrierStep) {
            prePredecessor = prePredecessor.getPreviousStep();
        }

        // match preceding inV(), outV() or otherV() steps
        // predecessor has to operate on an edge type
        if ((predecessor instanceof EdgeVertexStep || predecessor instanceof EdgeOtherVertexStep) &&
            (prePredecessor instanceof VertexStep) && ((VertexStep<?>) prePredecessor).returnsEdge()) {
            return OptimizablePosition.V2E_E2V_ID;
        }

        return OptimizablePosition.NONE;
    }

    private void replaceSequence(T originalStep, OptimizablePosition pos) {
        switch (pos) {
        case V2E_E2V_ID:
            replaceSequenceV2EthenE2VthenID(originalStep);
            break;
        case V2V_ID:
            replaceSequenceV2VthenID(originalStep);
            break;
        default:
            break;
        }
    }

    private void replaceSequenceV2EthenE2VthenID(T originalStep) {
        Traversal.Admin<?,?> traversal = originalStep.getTraversal();

        Step<Edge,Vertex> e2vStep;
        // remove obsolete NoOpBarrier
        if (originalStep.getPreviousStep() instanceof NoOpBarrierStep) {
            traversal.removeStep(originalStep.getPreviousStep());
        }
        e2vStep = (Step<Edge, Vertex>) originalStep.getPreviousStep();
        originalStep.getLabels().forEach(e2vStep::addLabel);

        // remove original selection step
        traversal.removeStep(originalStep);

        // create new has("~adjacent", id_value) step before e2v step
        FilterStep<Edge> filterByAdjacentIdStep = makeFilterByAdjacentIdStep(traversal, originalStep);

        // insert new step
        TraversalHelper.insertBeforeStep(filterByAdjacentIdStep, e2vStep, traversal);

        // remove obsolete NoOpBarrier
        Step<?,?> v2eStep = filterByAdjacentIdStep.getPreviousStep();
        while (v2eStep instanceof NoOpBarrierStep) {
            Step<?,?> barrierStep = v2eStep;
            v2eStep = barrierStep.getPreviousStep();
            traversal.removeStep(barrierStep);
        }
    }

    private void replaceSequenceV2VthenID(T originalStep) {
        Traversal.Admin<?,?> traversal = originalStep.getTraversal();

        // remove obsolete NoOpBarrier
        if (originalStep.getPreviousStep() instanceof NoOpBarrierStep) {
            traversal.removeStep(originalStep.getPreviousStep());
        }

        // create new V2E step based on old V2V step
        VertexStep<?> v2vStep = (VertexStep<?>) originalStep.getPreviousStep();
        String[] edgeLabels = v2vStep.getEdgeLabels();
        Direction v2vDirection = v2vStep.getDirection();
        VertexStep<Edge> v2eStep = new VertexStep<>(traversal, Edge.class, v2vDirection, edgeLabels);

        // create new E2V step based on old V2V step
        Step<Edge, Vertex> e2vStep;
        if (v2vDirection == Direction.BOTH) {
            e2vStep = new EdgeOtherVertexStep(traversal);
        } else {
            e2vStep = new EdgeVertexStep(traversal, v2vDirection.opposite());
        }
        originalStep.getLabels().forEach(e2vStep::addLabel);

        Step<?, Vertex> predecessor = v2vStep.getPreviousStep();

        // drop old steps
        traversal.removeStep(originalStep);
        traversal.removeStep(v2vStep);

        // create new has("~adjacent", id_value) step before e2v step
        FilterStep<Edge> filterByAdjacentIdStep = makeFilterByAdjacentIdStep(traversal, originalStep);

        // insert new steps
        TraversalHelper.insertAfterStep(v2eStep, predecessor, traversal);
        TraversalHelper.insertAfterStep(filterByAdjacentIdStep, v2eStep, traversal);
        TraversalHelper.insertAfterStep(e2vStep, filterByAdjacentIdStep, traversal);
    }

    protected abstract FilterStep<Edge> makeFilterByAdjacentIdStep(Traversal.Admin<?, ?> traversal, T originalStep);
}
