package com.thinkaurelius.titan.graphdb.types.manager;

import com.thinkaurelius.titan.graphdb.types.TypeDefinition;

class TypeInformation {

	final TypeDefinition definition;
	final long definitionEdgeID;
	final long nameEdgeID;
	
	TypeInformation(TypeDefinition def, long defid, long nameid) {
		definition = def;
		definitionEdgeID = defid;
		nameEdgeID = nameid;
	}
	
}
