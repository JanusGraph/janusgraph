package org.janusgraph.graphdb.olap.computer;

import org.janusgraph.core.JanusTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraElementTraversal<S, E>  extends DefaultGraphTraversal<S, E> {

    private final JanusTransaction graph;

    private FulgoraElementTraversal(final JanusTransaction graph) {
        super(graph);
        this.graph=graph;
    }

    public static<S,E> FulgoraElementTraversal<S,E> of(final JanusTransaction graph) {
        return new FulgoraElementTraversal<>(graph);
    }

    @Override
    public Optional<Graph> getGraph() {
        return Optional.of(graph);
    }

}
