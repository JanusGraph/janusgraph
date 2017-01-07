package org.janusgraph.graphdb.transaction;

import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexIterable implements Iterable<InternalVertex> {

    private final StandardJanusGraphTx tx;
    private final StandardJanusGraph graph;

    public VertexIterable(final StandardJanusGraph graph, final StandardJanusGraphTx tx) {
        this.graph = graph;
        this.tx = tx;
    }

    @Override
    public Iterator<InternalVertex> iterator() {
        return new Iterator<InternalVertex>() {

            RecordIterator<Long> iterator = graph.getVertexIDs(tx.getTxHandle());
            InternalVertex nextVertex = nextVertex();

            private InternalVertex nextVertex() {
                InternalVertex v = null;
                while (v == null && iterator.hasNext()) {
                    long nextId = iterator.next().longValue();
                    //Filter out invisible vertices
                    if (IDManager.VertexIDType.Invisible.is(nextId)) continue;

                    v = tx.getInternalVertex(nextId);
                    //Filter out deleted vertices and types
                    if (v.isRemoved()) v = null;
                }
                return v;
            }

            @Override
            public boolean hasNext() {
                return nextVertex != null;
            }

            @Override
            public InternalVertex next() {
                if (!hasNext()) throw new NoSuchElementException();
                InternalVertex returnVertex = nextVertex;
                nextVertex = nextVertex();
                return returnVertex;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
