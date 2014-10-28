package com.thinkaurelius.titan.graphdb.blueprints;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphStepStrategy implements TraversalStrategy.NoDependencies {

    private static final TitanGraphStepStrategy INSTANCE = new TitanGraphStepStrategy();

    private TitanGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        final TitanGraphStep titanGraphStep = (TitanGraphStep) TraversalHelper.getStart(traversal);
        Step currentStep = titanGraphStep.getNextStep();
        while (true) {
            if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;

            if (currentStep instanceof HasStep) {
                titanGraphStep.hasContainers.addAll(((HasStep) currentStep).getHasContainers());
                TraversalHelper.removeStep(currentStep, traversal);
            } else if (currentStep instanceof IntervalStep) {
                titanGraphStep.hasContainers.addAll(((IntervalStep) currentStep).getHasContainers());
                TraversalHelper.removeStep(currentStep, traversal);
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    public static TitanGraphStepStrategy instance() {
        return INSTANCE;
    }
}
