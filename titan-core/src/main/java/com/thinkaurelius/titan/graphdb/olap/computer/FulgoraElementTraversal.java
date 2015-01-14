package com.thinkaurelius.titan.graphdb.olap.computer;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraElementTraversal<S, E>  extends DefaultGraphTraversal<S, E> {

    private final TitanTransaction graph;

    private FulgoraElementTraversal(final TitanTransaction graph) {
        super(FulgoraElementTraversal.class);
        this.graph=graph;
    }

    public static<S,E> FulgoraElementTraversal<S,E> of(final TitanTransaction graph) {
        return new FulgoraElementTraversal<>(graph);
    }

    public TitanTransaction getGraph() {
        return graph;
    }

}