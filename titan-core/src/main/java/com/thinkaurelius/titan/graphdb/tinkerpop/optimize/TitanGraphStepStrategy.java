package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphStepStrategy extends AbstractTraversalStrategy {

    private static final TitanGraphStepStrategy INSTANCE = new TitanGraphStepStrategy();

    private TitanGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        final Step<?, ?> startStep = TraversalHelper.getStart(traversal);
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            if (originalGraphStep.getIds()==null || originalGraphStep.getIds().length==0) {
                final TitanGraphStep<?> titanGraphStep = new TitanGraphStep<>(originalGraphStep);
                TraversalHelper.replaceStep(startStep, (Step)titanGraphStep, traversal);

                HasStepFolder.foldInHasContainer(titanGraphStep,traversal);
                HasStepFolder.foldInOrder(titanGraphStep,traversal,traversal,titanGraphStep.returnsVertices());
                HasStepFolder.foldInRange(titanGraphStep,traversal);
            }
        }
    }

    public static TitanGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return TitanTraversalUtil.PRIORS;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return TitanTraversalUtil.POSTS;
    }


}
