package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphStepStrategy extends AbstractTraversalStrategy {

    private static final TitanGraphStepStrategy INSTANCE = new TitanGraphStepStrategy();

    private TitanGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        final TitanGraphStep titanGraphStep = (TitanGraphStep) TraversalHelper.getStart(traversal);
        HasStepFolder.foldInHasContainer(titanGraphStep,traversal);
        HasStepFolder.foldInLastOrderBy(titanGraphStep,traversal,titanGraphStep.returnsVertices());
        HasStepFolder.foldInRange(titanGraphStep,traversal,RangeStep.class);
    }

    public static TitanGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return TitanTraversal.PRIORS;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return TitanTraversal.POSTS;
    }


}
