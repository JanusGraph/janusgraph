package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.strategy.IdentityRemovalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.LocalRangeStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static final Set<Class<? extends TraversalStrategy>> POSTS = ImmutableSet.of(TraverserSourceStrategy.class, LocalRangeStrategy.class);
    static final Set<Class<? extends TraversalStrategy>> PRIORS = ImmutableSet.of(IdentityRemovalStrategy.class);


    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        TraversalStrategies.GlobalCache.registerStrategies(TitanTraversal.class, traversalStrategies);
    }

    public TitanTraversal(final TitanTransaction graph) {
        super(graph);
        addStep(new StartStep<>(this));
    }


    /* ----------------------------------------------
                General Traversal Utility Methods
       ---------------------------------------------- */

    public static Step replaceStep(final Step step) {
        if (step instanceof VertexStep) {
            VertexStep vstep = (VertexStep)step;
            return new TitanVertexStep(vstep);
        } else if (step instanceof PropertiesStep) {
            PropertiesStep sstep = (PropertiesStep)step;
            return sstep;
//            return new TitanPropertiesStep<>(sstep);
        } else {
            return step;
        }
    }

    public static TitanTransaction getTx(Traversal traversal) {
        return (TitanTransaction)traversal.sideEffects().getGraph();
    }

}
