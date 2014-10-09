package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TitanTraversal(final TitanTransaction graph) {
        this.sideEffects().setGraph(graph);
        this.strategies().register(TitanGraphStepStrategy.instance());
        this.addStep(new StartStep<>(this));
    }

    @Override
    public void prepareForGraphComputer() {
        super.prepareForGraphComputer();
        this.strategies().unregister(TitanGraphStepStrategy.class);
    }
}
