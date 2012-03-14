package com.thinkaurelius.titan.graphdb.util;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;
import com.thinkaurelius.titan.core.RelationshipType;

public class NodeUtils {

	public static boolean edgeExists(Node start, Node end, RelationshipType reltype) {
		for (Relationship rel: start.getRelationships(reltype, Direction.Out)) {
			if (rel.isIncidentOn(end)) return true;
		}
		return false;
	}
	
}
