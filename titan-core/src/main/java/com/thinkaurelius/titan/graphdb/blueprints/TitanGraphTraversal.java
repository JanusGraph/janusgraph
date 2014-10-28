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
        getStrategies().register(TitanGraphStepStrategy.instance());
        addStep(new TitanGraphStep<>(this, elementClass));
    }

}
