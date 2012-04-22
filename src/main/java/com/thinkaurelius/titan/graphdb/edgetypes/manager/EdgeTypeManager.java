package com.thinkaurelius.titan.graphdb.edgetypes.manager;


import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeVisibility;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public interface EdgeTypeManager {

	public EdgeTypeMaker getEdgeTypeMaker(GraphTx tx);
	
	public RelationshipType createRelationshipType(GraphTx tx, String name, EdgeCategory category,
                                                   Directionality directionality, EdgeTypeVisibility visibility,
                                                   boolean isfunctional, EdgeType[] keysig,
                                                   EdgeType[] compactsig, EdgeTypeGroup group);

	public PropertyType createPropertyType(GraphTx tx, String name, EdgeCategory category,
                                           Directionality directionality, EdgeTypeVisibility visibility,
                                           boolean isfunctional, EdgeType[] keysig,
                                           EdgeType[] compactsig, EdgeTypeGroup group,
                                           boolean isKey, boolean hasIndex, Class<?> objectType);
	
	
	
	public InternalEdgeType getEdgeType(long id, GraphTx tx);
	
	public InternalEdgeType getEdgeType(String name, GraphTx tx);
	
	public boolean containsEdgeType(long id, GraphTx tx);
	
	public boolean containsEdgeType(String name, GraphTx tx);

	public void committed(InternalEdgeType edgetype);
	
	public void close();
	

}
