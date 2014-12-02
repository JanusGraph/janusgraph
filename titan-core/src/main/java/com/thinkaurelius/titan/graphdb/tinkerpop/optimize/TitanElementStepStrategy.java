package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.gremlin.process.*;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.LocalRangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderByStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Modeled after {@link com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerElementStepStrategy}
 *
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class TitanElementStepStrategy extends AbstractTraversalStrategy {

    private static final TitanElementStepStrategy INSTANCE = new TitanElementStepStrategy();

    private static final Set<Class<? extends TraversalStrategy>> POSTS =
            ImmutableSet.copyOf(Iterables.concat(TitanTraversal.POSTS,
                            ImmutableSet.of(TitanLocalQueryOptimizerStrategy.class)));

    private TitanElementStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        //Copied this block of code from TinkerElementStepStrategy: If traversal starts with a startstep => convert to graphstep
        //Only replaced TinkerGraphStep -> TitanGraphStep
        final StartStep<Element> startStep = (StartStep) TraversalHelper.getStart(traversal);
        if (startStep.startAssignableTo(Vertex.class, Edge.class)) {
            final Element element = ((StartStep<?>) startStep).getStart();
            final String label = startStep.getLabel();
            TraversalHelper.removeStep(startStep, traversal);
            if (TraversalHelper.isLabeled(label)) {
                final Step identityStep = new IdentityStep(traversal);
                identityStep.setLabel(label);
                TraversalHelper.insertStep(identityStep, 0, traversal);
            }
            TraversalHelper.insertStep(new HasStep(traversal, new HasContainer(T.id, Compare.eq, element.id())), 0, traversal);
            TraversalHelper.insertStep(new TitanGraphStep(traversal, element.getClass()), 0, traversal);
        }
    }

    public static TitanElementStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return TitanTraversal.PRIORS;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }


}
