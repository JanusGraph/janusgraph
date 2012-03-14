package com.thinkaurelius.titan.traversal;


/**
 * Defines the mode used when extracting and converting a traversed graph subgraph.
 * 
 * Some graph traversals, such as {@link JUNGTraversal}, copy and convert the traversed subgraph for further processing.
 * {@link com.thinkaurelius.titan.traversal.ConversionMode} specifies how this conversion should convert different edges encountered during traversal.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public enum ConversionMode { 
	
	/**
	 * Standard conversion preserves the edge directionality during conversion.
	 */
	Standard, 
	
	/**
	 * ForceUndirected converts all edges into undirected edges.
	 */
	ForceUndirected, 
	
	/**
	 * IgnoreUndirected only copies and converts directed edges, ignoring undirected ones.
	 */
	IgnoreUndirected;
	
}
