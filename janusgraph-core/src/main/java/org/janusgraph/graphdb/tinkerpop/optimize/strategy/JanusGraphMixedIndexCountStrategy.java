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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMixedIndexCountStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;

import java.util.Collections;
import java.util.Set;

/**
 * If the query can be satisfied by a single mixed index query, and the query is followed by a count step, then
 * this strategy replaces original step with {@link JanusGraphMixedIndexCountStep}, which fires a count query against
 * mixed index backend without retrieving all elements
 *
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class JanusGraphMixedIndexCountStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final JanusGraphMixedIndexCountStrategy INSTANCE = new JanusGraphMixedIndexCountStrategy();
    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = Collections.singleton(JanusGraphStepStrategy.class);

    private JanusGraphMixedIndexCountStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(JanusGraphStep.class, traversal).forEach(originalGraphStep -> {
            buildMixedIndexCountStep(originalGraphStep, traversal);
        });
    }

    /**
     * Check if a mixed index count step can be built, and if so, apply it.
     *
     * @param originalGraphStep
     * @param traversal
     */
    private void buildMixedIndexCountStep(final JanusGraphStep originalGraphStep, final Traversal.Admin<?, ?> traversal) {
        if (!originalGraphStep.isStartStep() || !hasCountGlobalStep(originalGraphStep)) {
            return;
        }
        // try to find a suitable mixed index and build a mixed index count query
        final JanusGraphMixedIndexCountStep<?> directQueryCountStep = new JanusGraphMixedIndexCountStep<>(originalGraphStep, traversal);
        if (directQueryCountStep.getMixedIndexCountQuery() != null) {
            applyMixedIndexCountStep(originalGraphStep, directQueryCountStep, traversal);
        }
    }

    private boolean isEligibleToSkip(final Step currentStep) {
        return currentStep instanceof IdentityStep || currentStep instanceof NoOpBarrierStep;
    }

    private boolean hasCountGlobalStep(final GraphStep originalGraphStep) {
        Step<?, ?> currentStep = originalGraphStep.getNextStep();
        while (isEligibleToSkip(currentStep)) {
            currentStep = currentStep.getNextStep();
        }
        return currentStep instanceof CountGlobalStep;
    }

    /**
     * Apply mixed index count step in the traversal and remove "has" steps that already folded in the mixedIndexCountStep
     *
     * @param originalGraphStep
     * @param mixedIndexCountStep
     * @param traversal
     */
    private void applyMixedIndexCountStep(final GraphStep originalGraphStep, final JanusGraphMixedIndexCountStep mixedIndexCountStep,
                                          final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> currentStep = originalGraphStep.getNextStep();
        while (isEligibleToSkip(currentStep)) {
            currentStep = currentStep.getNextStep();
        }
        assert currentStep instanceof CountGlobalStep;
        traversal.removeStep(currentStep);
        TraversalHelper.replaceStep(originalGraphStep, mixedIndexCountStep, traversal);
    }

    public static JanusGraphMixedIndexCountStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
