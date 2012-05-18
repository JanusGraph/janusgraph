package com.thinkaurelius.titan.util.traversal;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.graphdb.edges.InternalRelation;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Defines an {@link java.util.Iterator} over all {@link com.thinkaurelius.titan.core.TitanEdge}s connecting a provided set of nodes.
 * 
 * Given a set of nodes, one may be interested in all relationships that are contained in the subgraph spanned
 * by those nodes. This iterator will return these relationships.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class AllEdgesIterator implements Iterator<Edge> {

	private final Set<? extends Vertex> nodes;
	private final Iterator<? extends Vertex> nodeIter;
	private Vertex currentNode=null;

	private Iterator<Edge> currentRel=Iterators.emptyIterator();
	
	private Edge next;
	
	/**
	 * Returns an iterator over all relationships incident on the nodes returned by the given Iterable over nodes.
	 * 
	 * Note that this method assumes that the given Iterable will return all nodes in the connected component,
	 * otherwise the behavior of this method is undefined.
	 * 
	 * @param nodeIter Iterator over a set of nodes defining a connected component.
	 */
	public AllEdgesIterator(Iterator<? extends Vertex> nodeIter) {
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
	public AllEdgesIterator(Set<? extends Vertex> nodes) {
		this.nodeIter=nodes.iterator();
		this.nodes = nodes;
		next = findNext();
	}
	
	private Edge findNext() {
		Edge rel = null;
		while (rel==null) {
			if (currentRel.hasNext()) {
				rel = currentRel.next();
				if (nodes!=null && !nodes.contains(rel.getVertex(Direction.IN)))
					rel = null;
			} else {
				currentRel = currentNode.getEdges(Direction.OUT).iterator();
			}
		}
		return rel;
	}
	
	@Override
	public boolean hasNext() {
		return next!=null;
	}

	@Override
	public Edge next() {
		if (next==null) throw new NoSuchElementException();
		Edge current = next;
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
