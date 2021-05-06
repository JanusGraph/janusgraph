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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Set;

/**
 * Constructs {@link Iterable}s over all {@link org.janusgraph.core.JanusGraphEdge}s connecting a provided set of vertices.
 * <p>
 * Given a set of vertices, one may be interested in all edges that are contained in the subgraph spanned
 * by those vertices.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
@Deprecated
public class AllEdgesIterable {

    private AllEdgesIterable() {
    }

    /**
     * Returns an iterable over all edges incident on the vertices returned by the given Iterable over vertices.
     * <p>
     * Note that this method assumes that the given Iterable will return all vertices in the connected component,
     * otherwise the behavior of this method is undefined.
     *
     * @param vertices Iterable over a set of vertices defining a connected component.
     * @return Iterable over all edges contained in this component.
     */
    public static Iterable<Edge> of(Iterable<? extends Vertex> vertices) {
        return new IterableBased(vertices);
    }

    /**
     * Returns an iterable over all edges contained in the subgraph spanned by the given vertices.
     * <p>
     * This method will return all edges whose end points are contained in the given set of vertices.
     *
     * @param vertices Set of vertices
     * @return All edges contained in the subgraph spanned by the set of vertices.
     */
    public static Iterable<Edge> of(Set<? extends Vertex> vertices) {
        return new SetBased(vertices);
    }


    private static class IterableBased implements Iterable<Edge> {

        private final Iterable<? extends Vertex> vertices;

        public IterableBased(Iterable<? extends Vertex> vertices) {
            this.vertices = vertices;
        }

        @Override
        public Iterator<Edge> iterator() {
            return new AllEdgesIterator(vertices.iterator());
        }

    }

    private static class SetBased implements Iterable<Edge> {

        private final Set<? extends Vertex> vertices;

        public SetBased(Set<? extends Vertex> vertices) {
            this.vertices = vertices;
        }

        @Override
        public Iterator<Edge> iterator() {
            return new AllEdgesIterator(vertices);
        }

    }

}
