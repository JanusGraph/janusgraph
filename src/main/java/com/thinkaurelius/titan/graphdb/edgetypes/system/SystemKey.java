package com.thinkaurelius.titan.graphdb.edgetypes.system;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.graphdb.edgetypes.*;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

public class SystemKey extends SystemType implements PropertyTypeDefinition, TitanKey {
	
	public static final SystemKey PropertyTypeDefinition =
		new SystemKey("PropertyTypeDefinition",StandardPropertyType.class,2);

	public static final SystemKey RelationshipTypeDefinition =
		new SystemKey("RelationshipTypeDefinition",StandardRelationshipType.class,3);

	public static final SystemKey TypeName =
		new SystemKey("TypeName",String.class,4,true);
	
	public static final SystemKey Attribute =
		new SystemKey("Attribute",Object.class,5);

    public static final SystemKey TypeClass =
            new SystemKey("TypeClass",TitanTypeClass.class,6,true);
	
	public static final Iterable<SystemKey> values() {
		return ImmutableList.of(PropertyTypeDefinition,RelationshipTypeDefinition,TypeName,Attribute,TypeClass);
	}
	
	private final Class<?> dataType;
	private final boolean index;
	
	private SystemKey(String name, Class<?> dataType, int id) {
		this(name,dataType,id,false);
	}
	
	private SystemKey(String name, Class<?> dataType, int id, boolean index) {
		super(name, IDManager.getSystemPropertyKeyID(id));
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
	public boolean isUnique() {
		return index;
	}
	
	@Override
	public final boolean isPropertyKey() {
		return true;
	}
	
	@Override
	public final boolean isEdgeLabel() {
		return false;
	}
	
		
}
