package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.TypeGroup;

public class StandardEdgeLabel extends AbstractTypeDefinition implements EdgeLabelDefinition {

	public StandardEdgeLabel() {}
	
	public StandardEdgeLabel(String name, TypeCategory category,
                             Directionality directionality, TypeVisibility visibility,
                             FunctionalType isfunctional, String[] keysig,
                             String[] compactsig, TypeGroup group) {
		super(name, category, directionality, visibility, isfunctional,
				keysig, compactsig,group);
	}

}
