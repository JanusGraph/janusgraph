package com.thinkaurelius.titan.graphdb.types;


/**
 * Enumerates all possible directionalities or orientations of an {@link com.thinkaurelius.titan.core.TitanRelation}.
 * {@link #Directed}: The {@link com.thinkaurelius.titan.core.TitanRelation} is directed with start and end vertices.
 * {@link #Undirected}: The {@link com.thinkaurelius.titan.core.TitanRelation} is undirected and does not distinguish between start and end vertices as all vertices are considered equal.
 * {@link #Unidirected}: The {@link com.thinkaurelius.titan.core.TitanRelation} is directed but only traversable from start node to end node. In other words, a {@link #Unidirected} {@link com.thinkaurelius.titan.core.TitanRelation}
 * is a pointer. Head vertices are not aware of incident {@link #Unidirected} edges.
 * 
 * The directionality is an important characteristic of an {@link com.thinkaurelius.titan.core.TitanRelation} and defined via its {@link com.thinkaurelius.titan.core.TitanType}.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public enum Directionality {
	
	/**
	 * Represents directed {@link com.thinkaurelius.titan.core.TitanRelation}s with start and end vertices.
	 */
	Directed, 
	
	/**
	 * Represents undirected {@link com.thinkaurelius.titan.core.TitanRelation}s with indistinguishable vertices.
	 */
	Undirected,
	
	/**
	 * Represents directed {@link com.thinkaurelius.titan.core.TitanRelation}s which are only traversable from start to end vertices, i.e. pointers.
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
