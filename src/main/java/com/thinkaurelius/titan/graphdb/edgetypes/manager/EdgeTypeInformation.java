package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeDefinition;

class EdgeTypeInformation {

	final EdgeTypeDefinition definition;
	final long definitionEdgeID;
	final long nameEdgeID;
	
	EdgeTypeInformation(EdgeTypeDefinition def, long defid, long nameid) {
		definition = def;
		definitionEdgeID = defid;
		nameEdgeID = nameid;
	}
	
}
