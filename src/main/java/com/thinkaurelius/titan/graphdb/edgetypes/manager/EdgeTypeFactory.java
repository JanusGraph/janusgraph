package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalTitanType;
import com.thinkaurelius.titan.graphdb.edgetypes.PropertyTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.RelationshipTypeDefinition;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public interface EdgeTypeFactory {

	public TitanKey createNewPropertyKey(PropertyTypeDefinition def, InternalTitanTransaction tx);
	
	public TitanLabel createNewEdgeLabel(RelationshipTypeDefinition def, InternalTitanTransaction tx);

	public InternalTitanType createExistingType(long id, EdgeTypeInformation info, InternalTitanTransaction tx);
	
	//public InternalTitanType createExistingEdgeLabel(long id, RelationshipTypeDefinition def, InternalTitanTransaction tx);
	
	public InternalTitanType createExistingPropertyKey(long id, InternalTitanTransaction tx);
	
	public InternalTitanType createExistingEdgeLabel(long id, InternalTitanTransaction tx);

	
}
