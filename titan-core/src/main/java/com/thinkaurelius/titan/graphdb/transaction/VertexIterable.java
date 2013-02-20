package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.graphdb.database.InternalTitanGraph;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VertexIterable implements Iterable<InternalVertex> {

    private final InternalTitanTransaction tx;
    private final InternalTitanGraph graph;

    public VertexIterable(final InternalTitanGraph graph, final InternalTitanTransaction tx) {
        this.graph = graph;
        this.tx = tx;
    }

    @Override
    public Iterator<InternalVertex> iterator() {
        return new Iterator<InternalVertex>() {

            RecordIterator<Long> iterator = graph.getVertexIDs(tx);
            InternalVertex nextVertex = nextVertex();

            private InternalVertex nextVertex() {
                InternalVertex v = null;
                try {
                    while (v == null && iterator.hasNext()) {
                        long nextId = iterator.next().longValue();
                        if (tx.isDeletedVertex(nextId)) continue;
                        v = tx.getExistingVertex(nextId);
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
