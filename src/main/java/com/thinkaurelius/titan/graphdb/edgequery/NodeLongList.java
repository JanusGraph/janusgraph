package com.thinkaurelius.titan.graphdb.edgequery;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

import java.util.Iterator;

public class NodeLongList implements NodeListInternal {

	private final GraphTx tx;
	private final AbstractLongList nodes;
	private final boolean sorted;
	
	public NodeLongList(GraphTx tx) {
		this.tx=tx;
		nodes = new LongArrayList();
		sorted=false;
	}
	
	public NodeLongList(GraphTx tx, AbstractLongList nodes, boolean sorted) {
		this.tx = tx;
		this.nodes = nodes;
		this.sorted = sorted;
	}

	@Override
	public void add(Node n) {
		if (!n.hasID()) throw new InvalidNodeException("Neighboring node does not have an id.");
		if (sorted) Preconditions.checkArgument(n.getID()>=nodes.get(nodes.size()-1),"Nodes must be inserted in sorted order");
		nodes.add(n.getID());
	}

	@Override
	public long getID(int pos) {
		return nodes.get(pos);
	}

	@Override
	public AbstractLongList getIDs() {
		return nodes;
	}

	@Override
	public Node get(int pos) {
		return tx.getExistingNode(getID(pos));
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
		return new Iterator<Node>() {

			private int pos=-1;
			
			@Override
			public boolean hasNext() {
				return (pos+1)<size();
			}

			@Override
			public Node next() {
				pos++;
				return get(pos);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Nodes cannot be removed from neighborhood list.");
			}
			
		};
	}
	
}
