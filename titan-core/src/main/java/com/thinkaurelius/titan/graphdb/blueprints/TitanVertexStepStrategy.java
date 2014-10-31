package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeOtherVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.ArrayList;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanVertexStepStrategy implements TraversalStrategy.NoDependencies {

    private static final TitanVertexStepStrategy INSTANCE = new TitanVertexStepStrategy();

    private TitanVertexStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        TraversalHelper.getStepsOfClass(TitanVertexStep.class, traversal).forEach(step -> {
            if (step.isEdgeStep()) { //TODO: don't optimize if branchFactor is already set
                HasStepFolder.foldInHasContainer(step,traversal);
            }
            //TODO: add optimization for localRange
            if (step.getNextStep() instanceof RangeStep) { //If its a global limit, then each local limit should be at least as much
                RangeStep rstep = (RangeStep)step.getNextStep();
                int limit = QueryUtil.convertLimit(rstep.getHighRange());
                step.setLimit(QueryUtil.mergeLimits(limit, step.getLimit()));
            }
        });
    }

    public static TitanVertexStepStrategy instance() {
        return INSTANCE;
    }

}
