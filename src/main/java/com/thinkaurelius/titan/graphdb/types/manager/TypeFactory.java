package com.thinkaurelius.titan.graphdb.types.manager;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.types.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.types.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public interface TypeFactory {

	public TitanKey createNewPropertyKey(PropertyKeyDefinition def, InternalTitanTransaction tx);
	
	public TitanLabel createNewEdgeLabel(EdgeLabelDefinition def, InternalTitanTransaction tx);

	public InternalTitanType createExistingType(long id, TypeInformation info, InternalTitanTransaction tx);
	
	//public InternalTitanType createExistingEdgeLabel(long id, EdgeLabelDefinition def, InternalTitanTransaction tx);
	
	public InternalTitanType createExistingPropertyKey(long id, InternalTitanTransaction tx);
	
	public InternalTitanType createExistingEdgeLabel(long id, InternalTitanTransaction tx);

	
}
