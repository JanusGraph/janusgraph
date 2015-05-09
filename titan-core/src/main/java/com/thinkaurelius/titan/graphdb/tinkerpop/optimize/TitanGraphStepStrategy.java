package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.graphdb.tinkerpop.ElementUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final TitanGraphStepStrategy INSTANCE = new TitanGraphStepStrategy();

    private TitanGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if(!traversal.getGraph().isPresent())
            return;

        if (traversal.getEngine().isComputer())
            return;

        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            if (originalGraphStep.getIds()==null || originalGraphStep.getIds().length==0) {
                //Try to optimize for index calls
                final TitanGraphStep<?> titanGraphStep = new TitanGraphStep<>(originalGraphStep);
                TraversalHelper.replaceStep(startStep, (Step)titanGraphStep, traversal);

                HasStepFolder.foldInHasContainer(titanGraphStep,traversal);
                HasStepFolder.foldInOrder(titanGraphStep,traversal,traversal,titanGraphStep.returnsVertices());
                HasStepFolder.foldInRange(titanGraphStep,traversal);
            } else {
                //Make sure that any provided "start" elements are instantiated in the current transaction
                Object[] ids = originalGraphStep.getIds();
                ElementUtils.verifyArgsMustBeEitherIdorElement(ids);
                if (ids[0] instanceof Element) {
                    //GraphStep constructor ensures that the entire array is elements
                    final Object[] elementIds = new Object[ids.length];
                    for (int i = 0; i < ids.length; i++) {
                        elementIds[i]=((Element)ids[i]).id();
                    }
                    originalGraphStep.setIteratorSupplier(() -> (Iterator) (originalGraphStep.returnsVertices() ?
                            originalGraphStep.getTraversal().getGraph().get().vertices(elementIds) :
                            originalGraphStep.getTraversal().getGraph().get().edges(elementIds)));
                }
            }
        }
    }

    public static TitanGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return TitanTraversalUtil.PRIORS;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return TitanTraversalUtil.POSTS;
    }


}
