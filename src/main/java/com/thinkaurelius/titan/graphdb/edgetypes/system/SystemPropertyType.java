package com.thinkaurelius.titan.graphdb.edgetypes.system;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.graphdb.edgetypes.PropertyTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.StandardPropertyType;
import com.thinkaurelius.titan.graphdb.edgetypes.StandardRelationshipType;

public class SystemPropertyType extends SystemEdgeType implements PropertyTypeDefinition, PropertyType  {
	
	public static final SystemPropertyType PropertyTypeDefinition = 
		new SystemPropertyType("PropertyTypeDefinition",StandardPropertyType.class,2);

	public static final SystemPropertyType RelationshipTypeDefinition = 
		new SystemPropertyType("RelationshipTypeDefinition",StandardRelationshipType.class,3);

	public static final SystemPropertyType EdgeTypeName = 
		new SystemPropertyType("EdgeTypeName",String.class,4,true);
	
	public static final SystemPropertyType Attribute = 
		new SystemPropertyType("Attribute",Object.class,5);
	
	public static final Iterable<SystemPropertyType> values() {
		return ImmutableList.of(PropertyTypeDefinition,RelationshipTypeDefinition,EdgeTypeName,Attribute);
	}
	
	private final Class<?> dataType;
	private final boolean index;
	
	private SystemPropertyType(String name, Class<?> dataType, int id) {
		this(name,dataType,id,false);
	}
	
	private SystemPropertyType(String name, Class<?> dataType, int id, boolean index) {
		super(name,SystemEdgeTypeManager.getInternalPropertyID(id));
		this.dataType=dataType;
		this.index=index;
	}
	

	@Override
	public Class<?> getDataType() {
		return dataType;
	}
		
	@Override
	public boolean isFunctional() {
		return true;
	}
	
	@Override
	public Directionality getDirectionality() {
		return Directionality.Directed;
	}

	@Override
	public boolean hasIndex() {
		return index;
	}
	
	@Override
	public boolean isKeyed() {
		return index;
	}
	
	@Override
	public final boolean isPropertyType() {
		return true;
	}
	
	@Override
	public final boolean isRelationshipType() {
		return false;
	}
	
		
}
