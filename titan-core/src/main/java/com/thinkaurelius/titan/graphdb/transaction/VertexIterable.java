package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexIterable implements Iterable<InternalVertex> {

    private final StandardTitanTx tx;
    private final StandardTitanGraph graph;

    public VertexIterable(final StandardTitanGraph graph, final StandardTitanTx tx) {
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
                    v = tx.getExistingVertex(nextId);
                    //Filter out deleted vertices and types
                    if (v.isRemoved() || (v instanceof  TitanType)) v = null;
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
