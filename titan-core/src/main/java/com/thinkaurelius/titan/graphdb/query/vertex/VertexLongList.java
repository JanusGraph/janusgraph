package com.thinkaurelius.titan.graphdb.query.vertex;

import com.carrotsearch.hppc.LongArrayList;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.AbstractLongListUtil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An implementation of {@link VertexListInternal} that stores only the vertex ids
 * and simply wraps an {@link LongArrayList} and keeps a boolean flag to remember whether this list is in sort order.
 * In addition, we need a transaction reference in order to construct actual vertex references on request.
 * </p>
 * This is a more efficient way to represent a vertex result set but only applies to loaded vertices that have ids.
 * So, compared to {@link VertexArrayList} this is an optimization for the special use case that a vertex is loaded.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexLongList implements VertexListInternal {

    private final StandardTitanTx tx;
    private LongArrayList vertices;
    private boolean sorted;

    public VertexLongList(StandardTitanTx tx) {
        this(tx,new LongArrayList(10),true);
    }

    public VertexLongList(StandardTitanTx tx, LongArrayList vertices, boolean sorted) {
        assert !sorted || AbstractLongListUtil.isSorted(vertices);
        this.tx = tx;
        this.vertices = vertices;
        this.sorted = sorted;
    }

    @Override
    public void add(TitanVertex n) {
        if (!vertices.isEmpty()) sorted = sorted && vertices.get(vertices.size()-1)<=n.longId();
        vertices.add(n.longId());
    }

    @Override
    public long getID(int pos) {
        return vertices.get(pos);
    }

    @Override
    public LongArrayList getIDs() {
        return vertices;
    }

    @Override
    public TitanVertex get(int pos) {
        return tx.getInternalVertex(getID(pos));
    }

    @Override
    public void sort() {
        if (sorted) return;
        Arrays.sort(vertices.buffer,0,vertices.size());
        sorted = true;
    }

    @Override
    public boolean isSorted() {
        return sorted;
    }

    @Override
    public VertexList subList(int fromPosition, int length) {
        LongArrayList subList = new LongArrayList(length);
        subList.add(vertices.buffer, fromPosition, length);
        assert subList.size()==length;
        return new VertexLongList(tx,subList,sorted);
    }

    @Override
    public int size() {
        return vertices.size();
    }

    @Override
    public void addAll(VertexList vertexlist) {
        LongArrayList othervertexids = null;
        if (vertexlist instanceof VertexLongList) {
            othervertexids = ((VertexLongList) vertexlist).vertices;
        } else if (vertexlist instanceof VertexArrayList) {
            VertexArrayList other = (VertexArrayList) vertexlist;
            othervertexids = new LongArrayList(other.size());
            for (int i = 0; i < other.size(); i++) othervertexids.add(other.getID(i));
        } else {
            throw new IllegalArgumentException("Unsupported vertex-list: " + vertexlist.getClass());
        }
        if (sorted && vertexlist.isSorted()) {
            //Merge join
            vertices = AbstractLongListUtil.mergeSort(vertices,othervertexids);
        } else {
            sorted = false;
            vertices.add(othervertexids.buffer, 0, othervertexids.size());
        }
    }

    public VertexArrayList toVertexArrayList() {
        VertexArrayList list = new VertexArrayList(tx);
        for (int i=0;i<vertices.size();i++) {
            list.add(get(i));
        }
        return list;
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
                if (!hasNext()) throw new NoSuchElementException();
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
