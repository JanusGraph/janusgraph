package com.thinkaurelius.titan.graphdb.query;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class VertexArrayList implements VertexListInternal {

    private final ArrayList<TitanVertex> vertices;
    private boolean sorted = false;

    public VertexArrayList() {
        vertices = new ArrayList<TitanVertex>();
    }

    @Override
    public void add(TitanVertex n) {
        vertices.add(n);
    }

    @Override
    public long getID(int pos) {
        return vertices.get(pos).getID();
    }

    @Override
    public AbstractLongList getIDs() {
        return toLongList(vertices);
    }

    @Override
    public TitanVertex get(int pos) {
        return vertices.get(pos);
    }

    @Override
    public void sort() {
        if (sorted) return;
        Collections.sort(vertices);
        sorted = true;
    }

    @Override
    public int size() {
        return vertices.size();
    }

    @Override
    public void addAll(VertexList vertexlist) {
        Preconditions.checkArgument(vertexlist instanceof VertexArrayList, "Only supporting union of identical lists.");
        VertexArrayList other = (VertexArrayList) vertexlist;
        sorted = false;
        vertices.addAll(other.vertices);
    }

    @Override
    public Iterator<TitanVertex> iterator() {
        return Iterators.unmodifiableIterator(vertices.iterator());
    }

    private static final AbstractLongList toLongList(List<TitanVertex> vertices) {
        AbstractLongList result = new LongArrayList(vertices.size());
        for (TitanVertex n : vertices) {
            result.add(n.getID());
        }
        return result;
    }

}
