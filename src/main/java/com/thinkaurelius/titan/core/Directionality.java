
package com.thinkaurelius.titan.core;


/**
 * Enumerates all possible directionalities or orientations of an {@link Edge}. 
 * {@link #Directed}: The {@link Edge} is directed with start and end nodes.
 * {@link #Undirected}: The {@link Edge} is undirected and does not distinguish between start and end nodes as all nodes are considered equal.
 * {@link #Unidirected}: The {@link Edge} is directed but only traversable from start node to end node. In other words, a {@link #Unidirected} {@link Edge}
 * is a pointer. End nodes are not aware of incident {@link #Unidirected} edges.
 * 
 * The directionality is an important characteristic of an {@link Edge} and defined via its {@link EdgeType}.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public enum Directionality {
	
	/**
	 * Represents directed {@link Edge}s with start and end nodes.
	 */
	Directed, 
	
	/**
	 * Represents undirected {@link Edge}s with indistinguishable nodes.
	 */
	Undirected,
	
	/**
	 * Represents directed {@link Edge}s which are only traversable from start to end nodes, i.e. pointers.
	 */
	Unidirected;

	@Override
	public String toString() {
		switch(this) {
		case Directed: return "Directed";
		case Undirected: return "Undirected";
		case Unidirected: return "Unidirected";
		default: throw new AssertionError("Unexpected enum constant: " + this);
		}
	}
	

}
