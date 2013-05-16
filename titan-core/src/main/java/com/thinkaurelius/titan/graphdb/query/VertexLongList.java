package com.thinkaurelius.titan.graphdb.query;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

import java.util.Iterator;

public class VertexLongList implements VertexListInternal {

    private final StandardTitanTx tx;
    private final AbstractLongList vertices;
    private boolean sorted;

    public VertexLongList(StandardTitanTx tx) {
        this(tx, new LongArrayList(), false);
    }

    public VertexLongList(StandardTitanTx tx, AbstractLongList vertices) {
        this(tx, vertices, false);
    }

    private VertexLongList(StandardTitanTx tx, AbstractLongList vertices, boolean sorted) {
        this.tx = tx;
        this.vertices = vertices;
        this.sorted = sorted;
    }

    @Override
    public void add(TitanVertex n) {
        if (sorted)
            Preconditions.checkArgument(n.getID() >= vertices.get(vertices.size() - 1), "Vertices must be inserted in sorted order");
        vertices.add(n.getID());
    }

    @Override
    public long getID(int pos) {
        return vertices.get(pos);
    }

    @Override
    public AbstractLongList getIDs() {
        return vertices;
    }

    @Override
    public TitanVertex get(int pos) {
        return tx.getExistingVertex(getID(pos));
    }

    @Override
    public void sort() {
        if (sorted) return;
        vertices.sort();
        sorted = true;
    }

    @Override
    public int size() {
        return vertices.size();
    }

    @Override
    public void addAll(VertexList vertexlist) {
        AbstractLongList othervertexids = null;
        if (vertexlist instanceof VertexLongList) {
            othervertexids = ((VertexLongList) vertexlist).vertices;
        } else if (vertexlist instanceof VertexArrayList) {
            VertexArrayList other = (VertexArrayList) vertexlist;
            othervertexids = new LongArrayList(other.size());
            for (int i = 0; i < other.size(); i++) othervertexids.add(other.getID(i));
        } else {
            throw new IllegalArgumentException("Unsupported vertex-list: " + vertexlist.getClass());
        }
        sorted = false;
        vertices.addAllOfFromTo(othervertexids, 0, othervertexids.size() - 1);
    }

    @Override
    public Iterator<TitanVertex> iterator() {
        return new Iterator<TitanVertex>() {

            private int pos = -1;

            @Override
            public boolean hasNext() {
                return (pos + 1) < size();
            }

            @Override
            public TitanVertex next() {
                pos++;
                return get(pos);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Vertices cannot be removed from neighborhood list");
            }

        };
    }

}
