package com.thinkaurelius.titan.graphdb.types;

public interface PropertyKeyDefinition extends TypeDefinition {

	public Class<?> getDataType();
	
	public boolean hasIndex();
	
	public boolean isUnique();
	
}
