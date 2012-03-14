package com.thinkaurelius.titan.traversal;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;

import java.util.Iterator;
import java.util.Set;

/**
 * Collects all nodes visited during a user specified traversal operation.
 * 
 * In addition, this class also provides the functionality to iterate over all relationships that are contained in
 * the subgraph spanned by the visited nodes.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class NodeTraversal extends AbstractTraversal<NodeTraversal> {

	NodeTraversal() {
		setParent(this);
	}

	/**
	 * Returns the set of nodes visited during this traversal starting from a set of seed nodes.
	 * 
	 * @param seed Set of seed nodes.
	 * @return Set of nodes visited during traversal
	 */
	public Set<Node> traversal(Set<Node> seed) {
		return super.traversal(seed, eval);
	}
	
	/**
	 * Returns the set of nodes visited during this traversal starting from a single seed node.
	 * 
	 * @param seed Seed node
	 * @return Set of nodes visited during traversal
	 */
	public Set<Node> traversal(Node seed) {
		return traversal(ImmutableSet.of(seed));
	}
	
	/**
	 * Returns an iterator over the nodes visited during traversal from a set of seed nodes.
	 * 
	 * @param seed Set of seed nodes.
	 * @return Iterator over nodes visited during traversal
	 */
	public Iterator<Node> traverse(Set<Node> seed) {
		return super.traverse(seed, eval);
	}
	
	/**
	 * Returns an iterator over the nodes visited during traversal from a single seed node.
	 * 
	 * @param seed Seed node
	 * @return Iterator over nodes visited during traversal
	 */
	public Iterator<Node> traverse(Node seed) {
		return traverse(ImmutableSet.of(seed));
	}
	
	/**
	 * Returns an iterator over the relationships contained in the subgraph which is spanned by the nodes visited during the traversal.
	 * The traversal starts from the set of seed nodes.
	 * Note, that all relationships connecting visited nodes are returned - not just those that were used during the traversal.
	 * 
	 * @param seed Set of seed nodes.
	 * @return Iterator over relationships connecting visited nodes.
	 */
	public Iterator<Relationship> traverseRelationships(Set<Node> seed) {
		return new AllRelationshipsIterator(traverse(seed));
	}
	
	/**
	 * Returns an iterator over the relationships contained in the subgraph which is spanned by the nodes visited during the traversal.
	 * The traversal starts from a single seed node.
	 * Note, that all relationships connecting visited nodes are returned - not just those that were used during the traversal.
	 * 
	 * @param seed Seed node
	 * @return Iterator over relationships connecting visited nodes.
	 */
	public Iterator<Relationship> traverseRelationships(Node seed) {
		return new AllRelationshipsIterator(traverse(seed));
	}

	private static final TraversalEvaluator eval = new TraversalEvaluator() {

		@Override
		public boolean nextNode(Node node) {
			return true;
		}

		@Override
		public void nextRelationship(Relationship edge) {
			//Do nothing
		}
		
	};

}
