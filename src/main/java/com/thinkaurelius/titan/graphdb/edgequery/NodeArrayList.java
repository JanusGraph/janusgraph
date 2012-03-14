package com.thinkaurelius.titan.graphdb.edgequery;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NodeArrayList implements NodeListInternal {

	private final List<Node> nodes;
	private final boolean sorted = false;
	
	public NodeArrayList() {
		nodes = new ArrayList<Node>();
	}
	
	@Override
	public void add(Node n) {
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
	public Node get(int pos) {
		return nodes.get(pos);
	}

	@Override
	public boolean isSorted() {
		return sorted;
	}

	@Override
	public int size() {
		return nodes.size();
	}

	@Override
	public Iterator<Node> iterator() {
		return Iterators.unmodifiableIterator(nodes.iterator());
	}
	
	private static final AbstractLongList toLongList(List<Node> nodes) {
		AbstractLongList result = new LongArrayList(nodes.size());
		for (Node n : nodes) {
			if (!n.hasID()) throw new InvalidNodeException("Neighboring node does not have an id.");
			result.add(n.getID());
		}
		return result;
	}

}
