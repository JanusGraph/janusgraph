package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public interface InternalTitanType extends TitanType, InternalTitanVertex {
	
	public EdgeTypeDefinition getDefinition();

    public boolean isFunctionalLocking();
	
}
