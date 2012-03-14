package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public interface InternalEdgeType extends EdgeType, InternalNode {
	
	public EdgeTypeDefinition getDefinition();
	
}
