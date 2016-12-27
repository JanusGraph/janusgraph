package com.thinkaurelius.titan.graphdb.query.vertex;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;

import java.util.*;

/**
 * An implementation of {@link VertexListInternal} that stores the actual vertex references
 * and simply wraps an {@link ArrayList} and keeps a boolean flag to remember whether this list is in sort order.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexArrayList implements VertexListInternal {

    public static final Comparator<TitanVertex> VERTEX_ID_COMPARATOR = new Comparator<TitanVertex>() {
        @Override
        public int compare(TitanVertex o1, TitanVertex o2) {
            return Long.compare(o1.longId(),o2.longId());
        }
    };

    private final StandardTitanTx tx;
    private List<TitanVertex> vertices;
    private boolean sorted;

    private VertexArrayList(StandardTitanTx tx, List<TitanVertex> vertices, boolean sorted) {
        Preconditions.checkArgument(tx!=null && vertices!=null);
        this.tx = tx;
        this.vertices=vertices;
        this.sorted=sorted;
    }

    public VertexArrayList(StandardTitanTx tx) {
        Preconditions.checkNotNull(tx);
        this.tx=tx;
        vertices = new ArrayList<TitanVertex>();
        sorted = true;
    }


    @Override
    public void add(TitanVertex n) {
        if (!vertices.isEmpty()) sorted = sorted && (vertices.get(vertices.size()-1).longId()<=n.longId());
        vertices.add(n);
    }

    @Override
    public long getID(int pos) {
        return vertices.get(pos).longId();
    }

    @Override
    public LongArrayList getIDs() {
        return toLongList(vertices);
    }

    @Override
    public TitanVertex get(int pos) {
        return vertices.get(pos);
    }

    @Override
    public void sort() {
        if (sorted) return;
        Collections.sort(vertices,VERTEX_ID_COMPARATOR);
        sorted = true;
    }

    @Override
    public boolean isSorted() {
        return sorted;
    }

    @Override
    public VertexList subList(int fromPosition, int length) {
        return new VertexArrayList(tx,vertices.subList(fromPosition,fromPosition+length),sorted);
    }

    @Override
    public int size() {
        return vertices.size();
    }

    @Override
    public void addAll(VertexList vertexlist) {
        Preconditions.checkArgument(vertexlist instanceof VertexArrayList, "Only supporting union of identical lists.");
        VertexArrayList other = (vertexlist instanceof VertexArrayList)?(VertexArrayList)vertexlist:
                ((VertexLongList)vertexlist).toVertexArrayList();
        if (sorted && other.isSorted()) {
            //Merge sort
            vertices = (ArrayList)IterablesUtil.mergeSort(vertices, other.vertices, VERTEX_ID_COMPARATOR);
        } else {
            sorted = false;
            vertices.addAll(other.vertices);
        }
    }

    public VertexLongList toVertexLongList() {
        LongArrayList list = toLongList(vertices);
        return new VertexLongList(tx,list,sorted);
    }

    @Override
    public Iterator<TitanVertex> iterator() {
        return Iterators.unmodifiableIterator(vertices.iterator());
    }

    /**
     * Utility method used to convert the list of vertices into a list of vertex ids (assuming all vertices have ids)
     *
     * @param vertices
     * @return
     */
    private static final LongArrayList toLongList(List<TitanVertex> vertices) {
        LongArrayList result = new LongArrayList(vertices.size());
        for (TitanVertex n : vertices) {
            result.add(n.longId());
        }
        return result;
    }

}
