package com.thinkaurelius.titan.graphdb.edgetypes.manager;


import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgetypes.Directionality;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeCategory;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeVisibility;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalTitanType;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public interface EdgeTypeManager {

	public TypeMaker getEdgeTypeMaker(InternalTitanTransaction tx);
	
	public TitanLabel createRelationshipType(InternalTitanTransaction tx, String name, EdgeCategory category,
                                                   Directionality directionality, EdgeTypeVisibility visibility,
                                                   boolean isfunctional, TitanType[] keysig,
                                                   TitanType[] compactsig, TypeGroup group);

	public TitanKey createPropertyType(InternalTitanTransaction tx, String name, EdgeCategory category,
                                           Directionality directionality, EdgeTypeVisibility visibility,
                                           boolean isfunctional, TitanType[] keysig,
                                           TitanType[] compactsig, TypeGroup group,
                                           boolean isKey, boolean hasIndex, Class<?> objectType);
	
	
	
	public InternalTitanType getEdgeType(long id, InternalTitanTransaction tx);
	
	public InternalTitanType getEdgeType(String name, InternalTitanTransaction tx);
	
	public boolean containsEdgeType(long id, InternalTitanTransaction tx);
	
	public boolean containsEdgeType(String name, InternalTitanTransaction tx);

	public void committed(InternalTitanType edgetype);
	
	public void close();
	

}
