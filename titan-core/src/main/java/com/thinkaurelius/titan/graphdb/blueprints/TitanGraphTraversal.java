package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TitanGraphTraversal(final TitanTransaction graph, final Class<? extends Element> elementClass) {
        super(graph);
        this.strategies().register(TitanGraphStepStrategy.instance());
        this.addStep(new TitanGraphStep(this, elementClass));
    }

    @Override
    public void prepareForGraphComputer() {
        super.prepareForGraphComputer();
        this.strategies().unregister(TitanGraphStepStrategy.class);
    }
}
