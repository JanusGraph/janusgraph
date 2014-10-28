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
        super(graph);
        addStep(new StartStep<>(this));
    }

}
