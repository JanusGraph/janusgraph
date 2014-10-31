package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.util.HasContainer;

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
        Step nextStep = titanGraphStep.getNextStep();
        while (nextStep instanceof RangeStep) {
            RangeStep rstep = (RangeStep)nextStep;
            int limit = QueryUtil.convertLimit(rstep.getHighRange());
            titanGraphStep.setLimit(QueryUtil.mergeLimits(limit,titanGraphStep.getLimit()));
            if (rstep.getLowRange()==0) TraversalHelper.removeStep(rstep, traversal);

            if (nextStep.equals(titanGraphStep.getNextStep())) break;
            nextStep = titanGraphStep.getNextStep();
        }
    }


    public static void foldRangeStep(Traversal traversal, HasStepFolder baseStep, RangeStep rstep) {
        int limit = QueryUtil.convertLimit(rstep.getHighRange());
        baseStep.setLimit(QueryUtil.mergeLimits(limit,baseStep.getLimit()));
        if (rstep.getLowRange()==0) TraversalHelper.removeStep(rstep, traversal);
    }

    public static TitanGraphStepStrategy instance() {
        return INSTANCE;
    }
}
