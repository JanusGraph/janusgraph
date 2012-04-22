package com.thinkaurelius.titan.graphdb.edgetypes;

public interface PropertyTypeDefinition extends EdgeTypeDefinition {

	public Class<?> getDataType();
	
	public boolean hasIndex();
	
	public boolean isKeyed();
	
}
