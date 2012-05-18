package com.thinkaurelius.titan.graphdb.edgequery;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;

import java.util.*;

public class VertexArrayList implements VertexListInternal {

	private final ArrayList<TitanVertex> nodes;
	private boolean sorted = false;
	
	public VertexArrayList() {
		nodes = new ArrayList<TitanVertex>();
	}
	
	@Override
	public void add(TitanVertex n) {
		nodes.add(n);
	}
	
	@Override
	public long getID(int pos) {
		return nodes.get(pos).getID();
	}

	@Override
	public AbstractLongList getIDs() {
		return toLongList(nodes);
	}

	@Override
	public TitanVertex get(int pos) {
		return nodes.get(pos);
	}

	@Override
	public void sort() {
        if (sorted) return;
        Collections.sort(nodes,new Comparator<TitanVertex>() {
            @Override
            public int compare(TitanVertex node, TitanVertex node1) {
                return Longs.compare(node.getID(),node1.getID());
            }
        });
		sorted = true;
	}

	@Override
	public int size() {
		return nodes.size();
	}

    @Override
    public void addAll(VertexList nodelist) {
        Preconditions.checkArgument(nodelist instanceof VertexArrayList, "Only supporting union of identical lists.");
        VertexArrayList other = (VertexArrayList)nodelist;
        sorted=false;
        nodes.addAll(other.nodes);
    }

	@Override
	public Iterator<TitanVertex> iterator() {
		return Iterators.unmodifiableIterator(nodes.iterator());
	}
	
	private static final AbstractLongList toLongList(List<TitanVertex> nodes) {
		AbstractLongList result = new LongArrayList(nodes.size());
		for (TitanVertex n : nodes) {
			if (!n.hasID()) throw new InvalidNodeException("Neighboring node does not have an id.");
			result.add(n.getID());
		}
		return result;
	}

}
