package com.thinkaurelius.titan.graphdb.blueprints;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanVertexStepStrategy implements TraversalStrategy.NoDependencies {

    private static final TitanVertexStepStrategy INSTANCE = new TitanVertexStepStrategy();

    private TitanVertexStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
//        TraversalHelper.getStepsOfClass(TitanVertexStep.class, traversal).forEach(step -> {
//            Step temp = step.getNextStep();
//            while (temp instanceof HasContainerHolder) {
//                step.addHasContainers(((HasContainerHolder) temp).getHasContainers());
//                TraversalHelper.removeStep(temp, traversal);
//                temp = step.getNextStep();
//            }
//        });
    }

    public static TitanVertexStepStrategy instance() {
        return INSTANCE;
    }

}
