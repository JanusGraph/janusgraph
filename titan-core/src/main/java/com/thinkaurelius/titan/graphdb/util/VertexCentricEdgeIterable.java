package com.thinkaurelius.titan.graphdb.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexCentricEdgeIterable<R extends TitanRelation> implements Iterable<R> {

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

        private final Iterator<InternalVertex> vertexIter;
        private Iterator<TitanRelation> currentOutEdges;
        private TitanRelation nextEdge = null;

        public EdgeIterator() {
            this.vertexIter = vertices.iterator();
            if (vertexIter.hasNext()) {
                currentOutEdges = relationCategory.executeQuery(vertexIter.next().query().direction(Direction.OUT)).iterator();
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
                    currentOutEdges = relationCategory.executeQuery(vertexIter.next().query().direction(Direction.OUT)).iterator();
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
            TitanRelation returnEdge = nextEdge;
            getNextEdge();
            return (R)returnEdge;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
