// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.util;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphEdge;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Defines an {@link java.util.Iterator} over all {@link org.janusgraph.core.JanusGraphEdge}s connecting a provided set of vertices.
 * <p>
 * Given a set of vertices, one may be interested in all edges that are contained in the subgraph spanned
 * by those vertices. This iterator will return these edges.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
@Deprecated
public class AllEdgesIterator implements Iterator<Edge> {

    private final Set<? extends Vertex> vertices;
    private final Iterator<? extends Vertex> vertexIterator;

    private Iterator<Edge> currentEdges = Collections.emptyIterator();

    private Edge next;

    /**
     * Returns an iterator over all edges incident on the vertices returned by the given Iterable over vertices.
     * <p>
     * Note that this method assumes that the given Iterable will return all vertices in the connected component,
     * otherwise the behavior of this method is undefined.
     *
     * @param vertexIterator Iterator over a set of vertices defining a connected component.
     */
    public AllEdgesIterator(Iterator<? extends Vertex> vertexIterator) {
        this.vertexIterator = vertexIterator;
        this.vertices = null;
        next = findNext();
    }

    /**
     * Returns an iterator over all edges contained in the subgraph spanned by the given vertices.
     * <p>
     * This method will return all edges whose end points are contained in the given set of vertices.
     *
     * @param vertices Set of vertices
     */
    public AllEdgesIterator(Set<? extends Vertex> vertices) {
        this.vertexIterator = vertices.iterator();
        this.vertices = vertices;
        next = findNext();
    }

    private Edge findNext() {
        JanusGraphEdge rel = null;
        while (rel == null) {
            if (currentEdges.hasNext()) {
                rel = (JanusGraphEdge)currentEdges.next();
                if (vertices != null && !vertices.contains(rel.vertex(Direction.IN)))
                    rel = null;
            } else {
                if (vertexIterator.hasNext()) {
                    Vertex nextVertex = vertexIterator.next();
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
