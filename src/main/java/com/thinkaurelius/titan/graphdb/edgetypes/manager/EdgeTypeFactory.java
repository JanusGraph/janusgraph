package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.edgetypes.PropertyTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.RelationshipTypeDefinition;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public interface EdgeTypeFactory {

	public PropertyType createNewPropertyType(PropertyTypeDefinition def, GraphTx tx);
	
	public RelationshipType createNewRelationshipType(RelationshipTypeDefinition def, GraphTx tx);	

	public InternalEdgeType createExistingEdgeType(long id, EdgeTypeInformation info, GraphTx tx);
	
	//public InternalEdgeType createExistingRelationshipType(long id, RelationshipTypeDefinition def, GraphTx tx);
	
	public InternalEdgeType createExistingPropertyType(long id, GraphTx tx);
	
	public InternalEdgeType createExistingRelationshipType(long id, GraphTx tx);

	
}
