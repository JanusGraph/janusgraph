package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;
import com.thinkaurelius.titan.core.RelationshipType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Provides functionality to inspect the neighborhood of a given node.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 *
 */
public class NodeNeighborhood {

	/**
	 * Returns the set of all nodes connected to the given node
	 * @param node Node to retrieve neighborhood for
	 * @return Set of all nodes connected to the given node
	 */
	public static final	Set<Node> getNeighborhood(Node node) {
		return getNeighborhood(node, node.getRelationshipIterator());
	}
	
	/**
	 * Returns the set of all nodes connected to the given node by relationships in the given direction
	 * @param node Node to retrieve neighborhood for
	 * @param dir Direction to restrict retrieval to
	 * @return Set of all nodes connected to the given node
	 */
	public static final	Set<Node> getNeighborhood(Node node, Direction dir) {
		return getNeighborhood(node, node.getRelationshipIterator(dir));

	}
	
	/**
	 * Returns the set of all nodes connected to the given node by relationships of given type and in the given direction
	 * @param node Node to retrieve neighborhood for
	 * @param relType Relationship type to restrict retrieval to 
	 * @param dir Direction to restrict retrieval to
	 * @return Set of all nodes connected to the given node
	 */
	public static final	Set<Node> getNeighborhood(Node node, RelationshipType relType, Direction dir) {
		return getNeighborhood(node, node.getRelationshipIterator(relType,dir));
	}
	
	private static final Set<Node> getNeighborhood(Node node, Iterator<? extends Relationship> iter) {
		Set<Node> neighbors = new HashSet<Node>();
		while (iter.hasNext()) {
			Relationship rel = iter.next();
			neighbors.add(rel.getOtherNode(node));
		}
		return neighbors;
	}


}
