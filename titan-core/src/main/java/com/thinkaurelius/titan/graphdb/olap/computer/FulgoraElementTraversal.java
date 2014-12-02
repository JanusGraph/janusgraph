package com.thinkaurelius.titan.graphdb.olap.computer;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanLocalQueryOptimizerStrategy;
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanTraversal;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraElementTraversal<S, E>  extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(TitanLocalQueryOptimizerStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(FulgoraElementTraversal.class, traversalStrategies);
    }

    private FulgoraElementTraversal(final TitanTransaction graph) {
        super(graph);
    }

    public static<S,E> FulgoraElementTraversal<S,E> of(final TitanTransaction graph) {
        return new FulgoraElementTraversal<>(graph);
    }

    @Override
    public <E2> GraphTraversal<S, E2> addStep(Step<?, E2> step) {
        if (isLocked()) throw Exceptions.traversalIsLocked();
        return super.addStep(TitanTraversal.replaceStep(step));
    }

}