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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.RelationCategory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexCentricEdgeIterable<R extends JanusGraphRelation> implements Iterable<R> {

    private final Iterable<InternalVertex> vertices;
    private final RelationCategory relationCategory;

    public VertexCentricEdgeIterable(final Iterable<InternalVertex> vertices, final RelationCategory relationCategory) {
        Preconditions.checkArgument(vertices!=null && relationCategory!=null);
        this.vertices = vertices;
        this.relationCategory = relationCategory;
    }


    @Override
    public Iterator<R> iterator() {
        return new EdgeIterator();
    }


    private class EdgeIterator implements Iterator<R> {

        private final Iterator<InternalVertex> vertexIterator;
        private Iterator<JanusGraphRelation> currentOutEdges;
        private JanusGraphRelation nextEdge = null;

        public EdgeIterator() {
            this.vertexIterator = vertices.iterator();
            if (vertexIterator.hasNext()) {
                currentOutEdges = relationCategory.executeQuery(vertexIterator.next().query().direction(Direction.OUT)).iterator();
                getNextEdge();
            }
        }

        private void getNextEdge() {
            assert vertexIterator != null && currentOutEdges != null;
            nextEdge = null;
            while (nextEdge == null) {
                if (currentOutEdges.hasNext()) {
                    nextEdge = currentOutEdges.next();
                    break;
                } else if (vertexIterator.hasNext()) {
                    currentOutEdges = relationCategory.executeQuery(vertexIterator.next().query().direction(Direction.OUT)).iterator();
                } else break;
            }
        }

        @Override
        public boolean hasNext() {
            return nextEdge != null;
        }

        @Override
        public R next() {
            if (nextEdge == null) throw new NoSuchElementException();
            JanusGraphRelation returnEdge = nextEdge;
            getNextEdge();
            return (R)returnEdge;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
