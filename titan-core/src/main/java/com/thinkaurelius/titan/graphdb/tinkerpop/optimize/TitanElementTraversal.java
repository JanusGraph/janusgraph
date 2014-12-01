package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanElementTraversal<S, E>  extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(TitanLocalQueryOptimizerStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(TitanElementTraversal.class, traversalStrategies);
    }

    public TitanElementTraversal(final Element element, final TitanTransaction graph) {
        super(graph);
        addStep(new StartStep<>(this, element));
    }

    private TitanElementTraversal(final TitanTransaction graph) {
        super(graph);
    }

    public static<S,E> TitanElementTraversal<S,E> of(final TitanTransaction graph) {
        return new TitanElementTraversal<>(graph);
    }

    @Override
    public <E2> GraphTraversal<S, E2> addStep(Step<?, E2> step) {
        if (isLocked()) throw Exceptions.traversalIsLocked();
        return super.addStep(TitanTraversal.replaceStep(step));
    }

}