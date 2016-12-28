package com.thinkaurelius.titan.graphdb.util;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanEdge;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Defines an {@link java.util.Iterator} over all {@link com.thinkaurelius.titan.core.TitanEdge}s connecting a provided set of vertices.
 * <p/>
 * Given a set of vertices, one may be interested in all edges that are contained in the subgraph spanned
 * by those vertices. This iterator will return these edges.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class AllEdgesIterator implements Iterator<Edge> {

    private final Set<? extends Vertex> vertices;
    private final Iterator<? extends Vertex> vertexIter;

    private Iterator<Edge> currentEdges = Iterators.emptyIterator();

    private Edge next;

    /**
     * Returns an iterator over all edges incident on the vertices returned by the given Iterable over vertices.
     * <p/>
     * Note that this method assumes that the given Iterable will return all vertices in the connected component,
     * otherwise the behavior of this method is undefined.
     *
     * @param vertexIter Iterator over a set of vertices defining a connected component.
     */
    public AllEdgesIterator(Iterator<? extends Vertex> vertexIter) {
        this.vertexIter = vertexIter;
        this.vertices = null;
        next = findNext();
    }

    /**
     * Returns an iterator over all edges contained in the subgraph spanned by the given vertices.
     * <p/>
     * This method will return all edges whose end points are contained in the given set of vertices.
     *
     * @param vertices Set of vertices
     */
    public AllEdgesIterator(Set<? extends Vertex> vertices) {
        this.vertexIter = vertices.iterator();
        this.vertices = vertices;
        next = findNext();
    }

    private Edge findNext() {
        TitanEdge rel = null;
        while (rel == null) {
            if (currentEdges.hasNext()) {
                rel = (TitanEdge)currentEdges.next();
                if (vertices != null && !vertices.contains(rel.vertex(Direction.IN)))
                    rel = null;
            } else {
                if (vertexIter.hasNext()) {
                    Vertex nextVertex = vertexIter.next();
                    currentEdges = nextVertex.edges(Direction.OUT);
                } else break;
            }
        }
        return rel;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Edge next() {
        if (next == null) throw new NoSuchElementException();
        Edge current = next;
        next = findNext();
        return current;
    }

    /**
     * Removing edges is not supported!
     *
     * @throws UnsupportedOperationException if invoked
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removals are not supported");
    }

}
