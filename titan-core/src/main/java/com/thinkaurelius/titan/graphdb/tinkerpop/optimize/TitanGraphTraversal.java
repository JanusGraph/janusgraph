package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TitanGraphTraversal(final TitanTransaction graph, final Class<? extends Element> elementClass) {
        super(graph);
        getStrategies().register(TitanGraphStepStrategy.instance());
        getStrategies().register(TitanLocalQueryOptimizerStrategy.instance());
        addStep(new TitanGraphStep<>(this, elementClass));
    }

    @Override
    public <E2> GraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        if (this.getStrategies().complete()) throw Exceptions.traversalIsLocked();
        return super.addStep(TitanTraversal.replaceStep(step));
    }





}
