package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.graphdb.database.InternalTitanGraph;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VertexIterable implements Iterable<InternalTitanVertex> {

    private final InternalTitanTransaction tx;
    private final InternalTitanGraph graph;

    public VertexIterable(final InternalTitanGraph graph, final InternalTitanTransaction tx) {
        this.graph = graph;
        this.tx = tx;
    }

    @Override
    public Iterator<InternalTitanVertex> iterator() {
        return new Iterator<InternalTitanVertex>() {

            RecordIterator<Long> iterator = graph.getVertexIDs(tx);
            InternalTitanVertex nextVertex = nextVertex();

            private InternalTitanVertex nextVertex() {
                InternalTitanVertex v = null;
                try {
                    while (v == null && iterator.hasNext()) {
                        v = tx.getExistingVertex(iterator.next().longValue());
                        //Filter out types
                        if (v instanceof TitanType) v = null;
                    }
                } catch (StorageException e) {
                    throw new TitanException("Read exception on open iterator", e);
                }
                return v;
            }

            @Override
            public boolean hasNext() {
                return nextVertex != null;
            }

            @Override
            public InternalTitanVertex next() {
                if (!hasNext()) throw new NoSuchElementException();
                InternalTitanVertex returnVertex = nextVertex;
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
