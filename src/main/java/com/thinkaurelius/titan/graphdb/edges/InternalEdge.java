package com.thinkaurelius.titan.graphdb.edges;

import com.thinkaurelius.titan.core.Edge;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

/**
 * The abstract Edge class defines the standard interface for edges used
 * in DOGMA.
 * It declares a set of operations and access methods that all edge implementations
 * provide.
 * 
 * @author Matthias Broecheler (me@matthiasb.com);
 *
 */
public interface InternalEdge extends Edge, InternalNode
{
	
	InternalNode getNodeAt(int pos);
    
    int getArity();
		
	void forceDelete();		
	
	boolean isHidden();
	
	
	/**
	 * A edge is virtual if it is not persisted to disk directly - that is, they exist
	 * only in memory. 
	 * For instance, labels of labeled edges are inline.
	 * @return TRUE, if edge is virtual, else FALSE
	 */
	boolean isInline();
	
}
