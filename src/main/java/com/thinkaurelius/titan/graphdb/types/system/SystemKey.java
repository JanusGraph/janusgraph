package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

public class SystemKey extends SystemType implements PropertyKeyDefinition, TitanKey {
	
	public static final SystemKey PropertyTypeDefinition =
		new SystemKey("PropertyKeyDefinition",StandardPropertyKey.class,2);

	public static final SystemKey RelationshipTypeDefinition =
		new SystemKey("EdgeLabelDefinition",StandardEdgeLabel.class,3);

	public static final SystemKey TypeName =
		new SystemKey("TypeName",String.class,4,true,true,false);
	
	public static final SystemKey Attribute =
		new SystemKey("Attribute",Object.class,5);

    public static final SystemKey TypeClass =
            new SystemKey("TypeClass",TitanTypeClass.class,6,true,false,false);
    
    public static final SystemKey VertexState =
            new SystemKey("VertexState",Byte.class,7,false,false,true);
	
	public static final Iterable<SystemKey> values() {
		return ImmutableList.of(PropertyTypeDefinition,RelationshipTypeDefinition,TypeName,Attribute,TypeClass,VertexState);
	}
	
	private final Class<?> dataType;
	private final boolean index;
    private final boolean unique;
    private final boolean modifiable;
	
	private SystemKey(String name, Class<?> dataType, int id) {
		this(name,dataType,id,false,false,false);
	}
	
	private SystemKey(String name, Class<?> dataType, int id, boolean index, boolean unique, boolean modifiable) {
		super(name, IDManager.getSystemPropertyKeyID(id));
		this.dataType=dataType;
		this.index=index;
        this.unique=unique;
        this.modifiable=modifiable;
	}
	
    @Override
    public boolean isModifiable() {
        return modifiable;
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
    public boolean isFunctionalLocking() {
        return false;
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
		return unique;
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
