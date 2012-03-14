package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.PropertyIndex;

public interface PropertyTypeDefinition extends EdgeTypeDefinition {

	public Class<?> getDataType();
	
	public PropertyIndex getIndexType();
	
	public boolean isKeyed();
	
}
