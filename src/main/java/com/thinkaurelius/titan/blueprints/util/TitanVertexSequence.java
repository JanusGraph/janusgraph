package com.thinkaurelius.titan.blueprints.util;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.blueprints.TitanGraph;
import com.thinkaurelius.titan.blueprints.TitanVertex;
import com.thinkaurelius.titan.core.Node;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Vertex;

import java.util.Iterator;

public class TitanVertexSequence<T extends Vertex> implements CloseableSequence<TitanVertex> {

    private final Iterator<? extends Node> nodes;
    private final TitanGraph graph;

    public TitanVertexSequence(final TitanGraph graph) {
        this.graph = graph;
        this.nodes= Iterators.emptyIterator();
    }
    
    public TitanVertexSequence(final TitanGraph graph, final Iterator<? extends Node> nodes) {
        this.nodes=nodes;
        this.graph = graph;
    }
    
    public TitanVertexSequence(final TitanGraph graph, final Iterable<? extends Node> nodes) {
        this(graph,nodes.iterator());
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
        return new TitanVertex(nodes.next(), graph);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
