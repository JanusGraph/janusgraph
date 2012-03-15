package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.blueprints.util.TitanEdgeSequence;
import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.Node;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.MultiIterable;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.util.ArrayList;
import java.util.List;

public class TitanVertex extends TitanElement<Node> implements Vertex {


    public TitanVertex(final Node vertex) {
        super(vertex);
    }

    private Iterable<Edge> getEdges(final Direction direction, final String... labels) {
        if (labels.length == 0) {
            return new TitanEdgeSequence(element.getRelationshipIterator(direction));
        } else if (labels.length == 1) {
            return new TitanEdgeSequence(element.getRelationshipIterator(labels[0], direction));
        } else {
            final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
            for (final String label : labels) {
                edges.add(new TitanEdgeSequence(element.getRelationshipIterator(label, direction)));
            }
            return new MultiIterable<Edge>(edges);
        }
    }

    @Override
    public Iterable<Edge> getOutEdges(final String... labels) {
        return getEdges(Direction.Out, labels);
    }

    @Override
    public Iterable<Edge> getInEdges(final String... labels) {
        return getEdges(Direction.In, labels);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

}
