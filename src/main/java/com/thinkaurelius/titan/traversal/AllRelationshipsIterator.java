package com.thinkaurelius.titan.traversal;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Defines an {@link java.util.Iterator} over all {@link com.thinkaurelius.titan.core.Relationship}s connecting a provided set of nodes.
 * 
 * Given a set of nodes, one may be interested in all relationships that are contained in the subgraph spanned
 * by those nodes. This iterator will return these relationships.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class AllRelationshipsIterator implements Iterator<Relationship> {

	private final Set<? extends Node> nodes;
	private final Iterator<? extends Node> nodeIter;
	private Node currentNode=null;
	boolean loadFresh = true;
	
	private Iterator<Relationship> currentRel=Iterators.emptyIterator();
	
	private Relationship next;
	
	/**
	 * Returns an iterator over all relationships incident on the nodes returned by the given Iterable over nodes.
	 * 
	 * Note that this method assumes that the given Iterable will return all nodes in the connected component,
	 * otherwise the behavior of this method is undefined.
	 * 
	 * @param nodeIter Iterator over a set of nodes defining a connected component.
	 */
	public AllRelationshipsIterator(Iterator<? extends Node> nodeIter) {
		this.nodeIter=nodeIter;
		this.nodes = null;
		next = findNext();
	}
	
	/**
	 * Returns an iterator over all relationships contained in the subgraph spanned by the given nodes.
	 * 
	 * This method will return all relationships whose end points are contained in the given set of nodes.
	 * 
	 * @param nodes Set of nodes
	 */
	public AllRelationshipsIterator(Set<? extends Node> nodes) {
		this.nodeIter=nodes.iterator();
		this.nodes = nodes;
		next = findNext();
	}
	
	private Relationship findNext() {
		Relationship rel = null;
		while (rel==null) {
			if (currentRel.hasNext()) {
				rel = currentRel.next();
				if (!((InternalEdge)rel).getNodeAt(0).equals(currentNode) || 
						(nodes!=null && !nodes.containsAll(rel.getNodes())) )
					rel = null;
			} else if (loadFresh) {
				if (!nodeIter.hasNext()) break;
				currentNode = nodeIter.next();
				currentRel = currentNode.getRelationshipIterator(Direction.Undirected);
				loadFresh = false;
			} else if (!loadFresh) {
				currentRel = currentNode.getRelationshipIterator(Direction.Out);
				loadFresh = true;
			}
		}
		return rel;
	}
	
	@Override
	public boolean hasNext() {
		return next!=null;
	}

	@Override
	public Relationship next() {
		if (next==null) throw new NoSuchElementException();
		Relationship current = next;
		next = findNext();
		return current;
	}

	/**
	 * Removing relationships is not supported!
	 * @throws UnsupportedOperationException if invoked
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Removals are not supported!");
	}

}
