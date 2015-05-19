package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.graphdb.tinkerpop.ElementUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy> implements TraversalStrategy.VendorOptimizationStrategy {

    private static final TitanGraphStepStrategy INSTANCE = new TitanGraphStepStrategy();

    private TitanGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isComputer())
            return;

        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            if (originalGraphStep.getIds() == null || originalGraphStep.getIds().length == 0) {
                //Try to optimize for index calls
                final TitanGraphStep<?> titanGraphStep = new TitanGraphStep<>(originalGraphStep);
                TraversalHelper.replaceStep(startStep, (Step) titanGraphStep, traversal);

                HasStepFolder.foldInHasContainer(titanGraphStep, traversal);
                HasStepFolder.foldInOrder(titanGraphStep, traversal, traversal, titanGraphStep.returnsVertex());
                HasStepFolder.foldInRange(titanGraphStep, traversal);
            } else {
                //Make sure that any provided "start" elements are instantiated in the current transaction
                Object[] ids = originalGraphStep.getIds();
                ElementUtils.verifyArgsMustBeEitherIdorElement(ids);
                if (ids[0] instanceof Element) {
                    //GraphStep constructor ensures that the entire array is elements
                    final Object[] elementIds = new Object[ids.length];
                    for (int i = 0; i < ids.length; i++) {
                        elementIds[i] = ((Element) ids[i]).id();
                    }
                    originalGraphStep.setIteratorSupplier(() -> (Iterator) (originalGraphStep.returnsVertex() ?
                            originalGraphStep.getTraversal().getGraph().get().vertices(elementIds) :
                            originalGraphStep.getTraversal().getGraph().get().edges(elementIds)));
                }
            }
        }
    }

    public static TitanGraphStepStrategy instance() {
        return INSTANCE;
    }
}
