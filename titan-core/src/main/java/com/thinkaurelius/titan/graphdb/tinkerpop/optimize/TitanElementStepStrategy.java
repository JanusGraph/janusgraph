package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Set;

/**
 * Modeled after {@code com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerElementStepStrategy}
 *
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class TitanElementStepStrategy extends AbstractTraversalStrategy {

    private static final TitanElementStepStrategy INSTANCE = new TitanElementStepStrategy();

    private static final Set<Class<? extends TraversalStrategy>> POSTS =
            ImmutableSet.copyOf(Iterables.concat(TitanTraversalUtil.POSTS,
                            ImmutableSet.of(TitanLocalQueryOptimizerStrategy.class)));

    private TitanElementStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isStandard())
            return;

        //Copied this block of code from TinkerElementStepStrategy: If traversal starts with a startstep && OLAP => convert to graphstep
        final StartStep<Element> startStep = (StartStep<Element>)traversal.asAdmin().getStartStep();
        if (startStep.startAssignableTo(Vertex.class, Edge.class)) {
            final Element element = ((StartStep<?>) startStep).getStart();
            traversal.removeStep(startStep);
            startStep.getLabel().ifPresent(label -> {
                final Step identityStep = new IdentityStep(traversal);
                identityStep.setLabel(label);
                traversal.addStep(0, identityStep);
            });
            traversal.addStep(0, new GraphStep(traversal.asAdmin(), element.getClass(), element.id()));
        }
    }

    public static TitanElementStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return TitanTraversalUtil.PRIORS;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }


}
