package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.TypeGroup;

public class StandardRelationshipType extends AbstractEdgeTypeDefinition implements RelationshipTypeDefinition {

	public StandardRelationshipType() {}
	
	public StandardRelationshipType(String name, EdgeCategory category,
			Directionality directionality, EdgeTypeVisibility visibility,
			boolean isfunctional, String[] keysig,
			String[] compactsig, TypeGroup group) {
		super(name, category, directionality, visibility, isfunctional,
				keysig, compactsig,group);
	}

}
