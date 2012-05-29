package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

/**
 * The abstract TitanRelation class defines the standard interface for edges used
 * in DOGMA.
 * It declares a set of operations and access methods that all edge implementations
 * provide.
 * 
 * @author Matthias Broecheler (me@matthiasb.com);
 *
 */
public interface InternalRelation extends TitanRelation, InternalTitanVertex
{
	
	InternalTitanVertex getVertex(int pos);
    
    int getArity();
		
	void forceDelete();		
	
	boolean isHidden();
	
	
	/**
	 * A edge is virtual if it is not persisted to disk directly - that is, they exist
	 * only in memory. 
	 * For INSTANCE, labels of labeled edges are inline.
	 * @return TRUE, if edge is virtual, else FALSE
	 */
	boolean isInline();
	
}
