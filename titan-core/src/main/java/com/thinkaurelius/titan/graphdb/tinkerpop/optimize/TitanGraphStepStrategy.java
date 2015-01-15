package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;

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
                TraversalHelper.replaceStep(startStep, titanGraphStep, traversal);

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
