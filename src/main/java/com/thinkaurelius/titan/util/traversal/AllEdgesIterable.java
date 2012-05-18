package com.thinkaurelius.titan.util.traversal;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;
import java.util.Set;

/**
 * Constructs {@link Iterable}s over all {@link com.thinkaurelius.titan.core.TitanEdge}s connecting a provided set of nodes.
 * 
 * Given a set of nodes, one may be interested in all relationships that are contained in the subgraph spanned
 * by those nodes.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class AllEdgesIterable {

	private AllEdgesIterable() {}
	
	/**
	 * Returns an iterable over all relationships incident on the nodes returned by the given Iterable over nodes.
	 * 
	 * Note that this method assumes that the given Iterable will return all nodes in the connected component,
	 * otherwise the behavior of this method is undefined.
	 * 
	 * @param nodes Iterable over a set of nodes defining a connected component.
	 * @return Iterable over all relationships contained in this component.
	 */
	public static Iterable<Edge> of(Iterable<? extends Vertex> nodes) {
		return new IterableBased(nodes);
	}
	
	/**
	 * Returns an iterable over all relationships contained in the subgraph spanned by the given nodes.
	 * 
	 * This method will return all relationships whose end points are contained in the given set of nodes.
	 * 
	 * @param nodes Set of nodes
	 * @return All relationships contained in the subgraph spanned by the set of nodes.
	 */
	public static Iterable<Edge> of(Set<? extends Vertex> nodes) {
		return new SetBased(nodes);
	}

	
	private static class IterableBased implements Iterable<Edge> {
		
		private final Iterable<? extends Vertex> nodes;
		
		public IterableBased(Iterable<? extends Vertex> nodes) {
			this.nodes=nodes;
		}
		
		@Override
		public Iterator<Edge> iterator() {
			return new AllEdgesIterator(nodes.iterator());
		}
		
	}
	
	private static class SetBased implements Iterable<Edge> {
		
		private final Set<? extends Vertex> nodes;
		
		public SetBased(Set<? extends Vertex> nodes) {
			this.nodes=nodes;
		}
		
		@Override
		public Iterator<Edge> iterator() {
			return new AllEdgesIterator(nodes);
		}
		
	}
	
}
