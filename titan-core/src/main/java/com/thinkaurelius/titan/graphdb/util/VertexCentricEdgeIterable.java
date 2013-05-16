package com.thinkaurelius.titan.graphdb.util;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexCentricEdgeIterable implements Iterable<Edge> {

    private final Iterable<Vertex> vertices;

    public VertexCentricEdgeIterable(Iterable<Vertex> vertices) {
        Preconditions.checkNotNull(vertices);
        this.vertices = vertices;
    }


    @Override
    public Iterator<Edge> iterator() {
        return new EdgeIterator(vertices.iterator());
    }


    private static class EdgeIterator implements Iterator<Edge> {

        private final Iterator<Vertex> vertexIter;
        private Iterator<Edge> currentOutEdges;
        private Edge nextEdge = null;

        public EdgeIterator(Iterator<Vertex> vertexIter) {
            this.vertexIter = vertexIter;
            if (vertexIter.hasNext()) {
                currentOutEdges = vertexIter.next().getEdges(Direction.OUT).iterator();
                getNextEdge();
            }
        }

        private void getNextEdge() {
            assert vertexIter != null && currentOutEdges != null;
            nextEdge = null;
            while (nextEdge == null) {
                if (currentOutEdges.hasNext()) {
                    nextEdge = currentOutEdges.next();
                    break;
                } else if (vertexIter.hasNext()) {
                    currentOutEdges = vertexIter.next().getEdges(Direction.OUT).iterator();
                } else break;
            }
        }

        @Override
        public boolean hasNext() {
            return nextEdge != null;
        }

        @Override
        public Edge next() {
            if (nextEdge == null) throw new NoSuchElementException();
            Edge returnEdge = nextEdge;
            getNextEdge();
            return returnEdge;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
