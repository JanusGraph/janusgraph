package org.janusgraph.graphdb.olap.computer;

import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraElementTraversal<S, E>  extends DefaultGraphTraversal<S, E> {

    private final JanusGraphTransaction graph;

    private FulgoraElementTraversal(final JanusGraphTransaction graph) {
        super(graph);
        this.graph=graph;
    }

    public static<S,E> FulgoraElementTraversal<S,E> of(final JanusGraphTransaction graph) {
        return new FulgoraElementTraversal<>(graph);
    }

    @Override
    public Optional<Graph> getGraph() {
        return Optional.of(graph);
    }

}
