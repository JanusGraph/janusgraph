// Copyright 2020 JanusGraph Authors
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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMixedIndexAggStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;


/**
 * If the query can be satisfied by a single mixed index query, and the query is followed by an aggregation step, then
 * this strategy replaces original step with a step that fires the aggregation against mixed index backend without
 * retrieving all elements
 *
 * @author Thomas Franco (thomas@strangebee.com)
 */
abstract class AbstractJanusGraphMixedIndexAggStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(JanusGraphStep.class, traversal).forEach(originalGraphStep -> {
            buildMixedIndexAggStep(originalGraphStep, traversal);
        });
    }

    /**
     * Check if a mixed index aggregation step can be built, and if so, apply it.
     *
     * @param originalGraphStep
     * @param traversal
     */
    private void buildMixedIndexAggStep(final JanusGraphStep originalGraphStep, final Traversal.Admin<?, ?> traversal) {
        if (!originalGraphStep.isStartStep()) {
            return;
        }
        final Aggregation agg = getAggregation(originalGraphStep);
        if (agg == null) {
            return;
        }
        // try to find a suitable mixed index and build a mixed index aggregation query
        final JanusGraphMixedIndexAggStep<?> directQueryAggStep = new JanusGraphMixedIndexAggStep<>(originalGraphStep, traversal, agg);
        if (directQueryAggStep.getMixedIndexAggQuery() != null) {
            applyMixedIndexAggStep(originalGraphStep, directQueryAggStep, traversal, agg.getFieldName() != null);
        }
    }

    protected boolean isEligibleToSkip(final Step currentStep) {
        return currentStep instanceof IdentityStep || currentStep instanceof NoOpBarrierStep;
    }

    abstract Aggregation getAggregation(final GraphStep originalGraphStep);

    /**
     * Apply mixed index aggregation step in the traversal and remove "has" steps that already folded in the
     * mixedIndexAggStep
     *
     * @param originalGraphStep the step to replace
     * @param mixedIndexAggStep the new step
     * @param traversal the traversal
     * @param hasField indicates if the aggregation is based on a field (min, max, sum, avg) or not (count)
     */
    private void applyMixedIndexAggStep(final GraphStep originalGraphStep, final JanusGraphMixedIndexAggStep mixedIndexAggStep,
                                        final Traversal.Admin<?, ?> traversal, boolean hasField) {
        Step<?, ?> currentStep = originalGraphStep.getNextStep();
        while (isEligibleToSkip(currentStep)) {
            currentStep = currentStep.getNextStep();
        }
        Step<?, ?> nextStep = currentStep.getNextStep();
        traversal.removeStep(currentStep);
        if (hasField) traversal.removeStep(nextStep);
        TraversalHelper.replaceStep(originalGraphStep, mixedIndexAggStep, traversal);
    }
}
