package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.strategy.LocalRangeStrategy;
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
        HasStepFolder.foldInHasContainer(titanGraphStep,traversal);
        HasStepFolder.foldInLastOrderBy(titanGraphStep,traversal,titanGraphStep.returnsVertices());
        HasStepFolder.foldInRange(titanGraphStep,traversal,RangeStep.class);
    }

    public static TitanGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public int compareTo(TraversalStrategy ts) {
        return -1;
    }


}
