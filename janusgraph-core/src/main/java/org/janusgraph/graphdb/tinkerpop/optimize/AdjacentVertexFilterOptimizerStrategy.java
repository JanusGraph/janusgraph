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

import org.janusgraph.graphdb.types.system.ImplicitKey;
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
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AdjacentVertexFilterOptimizerStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AdjacentVertexFilterOptimizerStrategy INSTANCE = new AdjacentVertexFilterOptimizerStrategy();

    private AdjacentVertexFilterOptimizerStrategy() {
    }

    public static AdjacentVertexFilterOptimizerStrategy instance() {
        return INSTANCE;
    }


    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        TraversalHelper.getStepsOfClass(TraversalFilterStep.class, traversal).forEach(originalStep -> {
            // Check if this filter traversal matches the pattern: _.inV/outV/otherV.is(x)
            Traversal.Admin<?, ?> filterTraversal = (Traversal.Admin<?, ?>) originalStep.getLocalChildren().get(0);
            List<Step> steps = filterTraversal.getSteps();
            if (steps.size() == 2 &&
                    (steps.get(0) instanceof EdgeVertexStep || steps.get(0) instanceof EdgeOtherVertexStep) &&
                    (steps.get(1) instanceof IsStep)) {
                //Get the direction in which we filter on the adjacent vertex (or null if not a valid adjacency filter)
                Direction direction = null;
                if (steps.get(0) instanceof EdgeVertexStep) {
                    EdgeVertexStep evs = (EdgeVertexStep) steps.get(0);
                    if (evs.getDirection() != Direction.BOTH) direction = evs.getDirection();
                } else {
                    assert steps.get(0) instanceof EdgeOtherVertexStep;
                    direction = Direction.BOTH;
                }
                P predicate = ((IsStep) steps.get(1)).getPredicate();
                //Check that we have a valid direction and a valid vertex filter predicate
                if (direction != null && predicate.getBiPredicate() == Compare.eq && predicate.getValue() instanceof Vertex) {
                    Vertex vertex = (Vertex) predicate.getValue();

                    //Now, check that this step is preceded by VertexStep that returns edges
                    Step<?, ?> currentStep = originalStep.getPreviousStep();
                    while (currentStep != EmptyStep.instance()) {
                        if (!(currentStep instanceof HasStep) && !(currentStep instanceof IdentityStep)) {
                            break;
                        } //We can jump over other steps as we move backward
                        currentStep = currentStep.getPreviousStep();
                    }
                    if (currentStep instanceof VertexStep) {
                        VertexStep vertexStep = (VertexStep) currentStep;
                        if (vertexStep.returnsEdge()
                                && (direction == Direction.BOTH || direction.equals(vertexStep.getDirection().opposite()))) {
                            //Now replace the step with a has condition
                            TraversalHelper.replaceStep(originalStep,
                                    new HasStep(traversal,
                                            HasContainer.makeHasContainers(ImplicitKey.ADJACENT_ID.name(), P.eq(vertex))),
                                    traversal);
                        }
                    }

                }
            }

        });

    }
}
