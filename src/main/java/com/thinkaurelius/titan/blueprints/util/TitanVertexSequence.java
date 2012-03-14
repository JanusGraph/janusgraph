package com.thinkaurelius.titan.blueprints.util;

import com.thinkaurelius.titan.blueprints.TitanVertex;
import com.thinkaurelius.titan.core.Node;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Vertex;

import java.util.Iterator;

public class TitanVertexSequence<T extends Vertex> implements CloseableSequence<TitanVertex> {

    private final Iterator<? extends Node> nodes;

    public TitanVertexSequence(final Iterator<? extends Node> nodes) {
        this.nodes=nodes;
    }
    
    public TitanVertexSequence(final Iterable<? extends Node> nodes) {
        this(nodes.iterator());
    }

    @Override
    public void close() {
        //Do nothing
    }

    @Override
    public Iterator<TitanVertex> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return nodes.hasNext();
    }

    @Override
    public TitanVertex next() {
        return new TitanVertex(nodes.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove vertex.");
    }
}
