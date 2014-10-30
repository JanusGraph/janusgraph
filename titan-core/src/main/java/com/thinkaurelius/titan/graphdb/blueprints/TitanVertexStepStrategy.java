package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeOtherVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.util.HasContainer;

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
            HasStepFolder.foldInHasContainer(step,traversal);

            if (step.isEdgeStep()) {
                Step nextStep = step.getNextStep();
                if (nextStep instanceof EdgeOtherVertexStep
                 || (nextStep instanceof EdgeVertexStep && ((EdgeVertexStep)nextStep).getDirection()==step.getDirection().opposite())
                        ) {
                    TraversalHelper.removeStep(nextStep, traversal);
                    TraversalHelper.replaceStep(step, step.makeVertexStep(),traversal);
                }
            }
        });
    }

    public static TitanVertexStepStrategy instance() {
        return INSTANCE;
    }

}
