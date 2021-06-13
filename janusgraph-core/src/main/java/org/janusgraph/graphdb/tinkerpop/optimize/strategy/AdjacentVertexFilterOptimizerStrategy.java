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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AdjacentVertexFilterOptimizerStrategy
    extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AdjacentVertexFilterOptimizerStrategy INSTANCE =
        new AdjacentVertexFilterOptimizerStrategy();

    private AdjacentVertexFilterOptimizerStrategy() {}

    public static AdjacentVertexFilterOptimizerStrategy instance() {
        return INSTANCE;
    }

    private enum OptimizableQueryType {
        NONE,
        IS,
        HASID
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        TraversalHelper.getStepsOfClass(TraversalFilterStep.class, traversal)
            .forEach(originalStep -> {
                // Check if this filter traversal matches the pattern: _.inV/outV/otherV.is/hasId(x)
                Traversal.Admin<?, ?> filterTraversal =
                    (Traversal.Admin<?, ?>) originalStep.getLocalChildren().get(0);
                List<Step> subSteps = filterTraversal.getSteps();

                OptimizableQueryType type = analyzeSubSteps(subSteps);

                if (type != OptimizableQueryType.NONE) {
                    replaceStep(traversal, type, originalStep, subSteps);
                }
            });
    }

    private void replaceStep(Traversal.Admin<?, ?> traversal, OptimizableQueryType type,
                             TraversalFilterStep originalStep, List<Step> steps) {
        // Get the direction in which we filter on the adjacent vertex (or null if not a valid
        // adjacency filter)
        Direction direction = parseDirection(steps);
        P predicate = parsePredicate(type, steps);
        // Check that we have a valid direction and a valid vertex filter predicate
        if (direction != null && isValidPredicate(type, predicate) &&
            isPreviousStepValid(originalStep, direction)) {
            // Now replace the step with a has condition
            HasContainer hc =
                new HasContainer(ImplicitKey.ADJACENT_ID.name(), P.eq(predicate.getValue()));
            TraversalHelper.replaceStep(originalStep, new HasStep(traversal, hc), traversal);
        }
    }

    private OptimizableQueryType analyzeSubSteps(List<Step> steps) {
        if (steps.size() != 2) {
            return OptimizableQueryType.NONE;
        }

        boolean validFirstStep = (steps.get(0) instanceof EdgeVertexStep);
        validFirstStep = validFirstStep || (steps.get(0) instanceof EdgeOtherVertexStep);

        if (!validFirstStep) {
            return OptimizableQueryType.NONE;
        }

        if (steps.get(1) instanceof IsStep) {
            // Check if this filter traversal matches the pattern: _.inV/outV/otherV.is(x)
            return OptimizableQueryType.IS;
        } else if (steps.get(1) instanceof HasStep) {
            // Check if this filter traversal matches the pattern: _.inV/outV/otherV.hasId(x)
            HasStep<?> hasStep = (HasStep<?>) steps.get(1);
            List<HasContainer> hasContainers = hasStep.getHasContainers();
            if (hasContainers.size() != 1) {
                // TODO does it make sense to allow steps with > 1 containers here?
                return OptimizableQueryType.NONE;
            }

            HasContainer has = hasContainers.get(0);
            if (has.getKey().equals(T.id.getAccessor())) {
                return OptimizableQueryType.HASID;
            } else {
                return OptimizableQueryType.NONE;
            }
        } else {
            return OptimizableQueryType.NONE;
        }
    }

    private boolean isPreviousStepValid(TraversalFilterStep originalStep, Direction direction) {
        // check that this step is preceded by VertexStep that returns edges
        Step<?, ?> previousStep = originalStep.getPreviousStep();
        while (previousStep != EmptyStep.instance()) {
            if (!(previousStep instanceof HasStep) && !(previousStep instanceof IdentityStep)) {
                break;
            }
            previousStep = previousStep.getPreviousStep();
        }

        if (previousStep instanceof VertexStep) {
            VertexStep<?> vertexStep = (VertexStep<?>) previousStep;
            if (vertexStep.returnsEdge() &&
                (direction == Direction.BOTH ||
                 direction.equals(vertexStep.getDirection().opposite()))) {
                return true;
            }
        }
        return false;
    }

    private Direction parseDirection(List<Step> steps) {
        if (steps.get(0) instanceof EdgeVertexStep) {
            EdgeVertexStep evs = (EdgeVertexStep) steps.get(0);
            if (evs.getDirection() != Direction.BOTH) {
                return evs.getDirection();
            }
            return null;
        } else {
            assert steps.get(0) instanceof EdgeOtherVertexStep;
            return Direction.BOTH;
        }
    }

    private P parsePredicate(OptimizableQueryType type, List<Step> steps) {
        switch (type) {
        case IS:
            IsStep isStep = (IsStep) steps.get(1);
            return isStep.getPredicate();
        case HASID:
            HasStep hasStep = (HasStep) steps.get(1);
            HasContainer hasContainer = (HasContainer) hasStep.getHasContainers().get(0);
            return hasContainer.getPredicate();
        default:
            return null;
        }
    }

    private boolean isValidPredicate(OptimizableQueryType type, P predicate) {
        if (predicate.getBiPredicate() != Compare.eq) {
            // only equality predicates are allowed
            return false;
        }

        switch (type) {
        case IS:
            return predicate.getValue() instanceof Vertex;
        case HASID:
            boolean vertexPredicate = predicate.getValue() instanceof Vertex;
            boolean idPredicate = predicate.getValue() instanceof Long;
            return vertexPredicate || idPredicate;
        default:
            return false;
        }
    }
}
