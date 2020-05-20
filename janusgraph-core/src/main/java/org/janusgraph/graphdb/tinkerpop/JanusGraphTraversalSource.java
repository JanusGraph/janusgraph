package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;


public class JanusGraphTraversalSource extends GraphTraversalSource{
    
    public JanusGraphTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public JanusGraphTraversalSource(final Graph graph) {
        super(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
    }

    public JanusGraphTraversalSource(final RemoteConnection connection) {
        super(connection);
    }

    public GraphTraversal<Edge, Edge> addE(final String relationId, final String label) {
        return addE(label).property(T.id, relationId);
    }
}