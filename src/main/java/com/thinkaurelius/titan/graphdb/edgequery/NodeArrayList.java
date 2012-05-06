package com.thinkaurelius.titan.graphdb.edgequery;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.NodeList;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;

import java.util.*;

public class NodeArrayList implements NodeListInternal {

	private final ArrayList<Node> nodes;
	private boolean sorted = false;
	
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
	public void sort() {
        if (sorted) return;
        Collections.sort(nodes,new Comparator<Node>() {
            @Override
            public int compare(Node node, Node node1) {
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
    public void addAll(NodeList nodelist) {
        Preconditions.checkArgument(nodelist instanceof NodeArrayList, "Only supporting union of identical lists.");
        NodeArrayList other = (NodeArrayList)nodelist;
        sorted=false;
        nodes.addAll(other.nodes);
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
