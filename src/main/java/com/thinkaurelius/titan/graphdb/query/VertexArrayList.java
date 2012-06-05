package com.thinkaurelius.titan.graphdb.query;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;

import java.util.*;

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
        Collections.sort(vertices,new Comparator<TitanVertex>() {
            @Override
            public int compare(TitanVertex node, TitanVertex node1) {
                return Longs.compare(node.getID(),node1.getID());
            }
        });
		sorted = true;
	}

	@Override
	public int size() {
		return vertices.size();
	}

    @Override
    public void addAll(VertexList nodelist) {
        Preconditions.checkArgument(nodelist instanceof VertexArrayList, "Only supporting union of identical lists.");
        VertexArrayList other = (VertexArrayList)nodelist;
        sorted=false;
        vertices.addAll(other.vertices);
    }

	@Override
	public Iterator<TitanVertex> iterator() {
		return Iterators.unmodifiableIterator(vertices.iterator());
	}
	
	private static final AbstractLongList toLongList(List<TitanVertex> vertices) {
		AbstractLongList result = new LongArrayList(vertices.size());
		for (TitanVertex n : vertices) {
			if (!n.hasID()) throw new InvalidElementException("Neighboring node does not have an id",n);
			result.add(n.getID());
		}
		return result;
	}

}
