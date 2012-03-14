package com.thinkaurelius.titan.graphdb.edgetypes.system;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.edgetypes.RelationshipTypeDefinition;

public class SystemRelationshipType extends SystemEdgeType implements RelationshipTypeDefinition, RelationshipType {
	
	//public static final SystemRelationshipType OtherNode = new SystemRelationshipType("OtherNode",1);	

	public static final SystemRelationshipType Start = new SystemRelationshipType("Start",2);	

	public static final SystemRelationshipType End = new SystemRelationshipType("End",3);	

	public static final SystemRelationshipType EdgeType = new SystemRelationshipType("EdgeType",4);	

	public static final Iterable<SystemRelationshipType> values() {
		return ImmutableList.of(Start,End,EdgeType);
	}
	
	
	private SystemRelationshipType(String name, int id) {
		super(name,SystemEdgeTypeManager.getInternalRelationshipID(id));
	}

	@Override
	public boolean isFunctional() {
		return true;
	}

	@Override
	public Directionality getDirectionality() {
		return Directionality.Unidirected;
	}
	
	@Override
	public final boolean isPropertyType() {
		return false;
	}
	
	@Override
	public final boolean isRelationshipType() {
		return true;
	}
	

	
}
