package com.thinkaurelius.titan.graphdb.edges.factory;

import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;


/**
 * Factory interface for creation of existing edges.
 * 
 * An edge (property or relationship) <b>exists</b> if it is persisted to external memory. Calling these
 * factory methods creates an in-memory representation of such persisted edges.
 * 
 * If existing edges do not have an id, the are <b>inline</b> edges, i.e. they are not persisted in their own right
 * but inline with another edge.
 *
 */
public interface EdgeLoader {

	InternalEdge createExistingProperty(long id, PropertyType type, InternalNode node, Object attribute);

	InternalEdge createExistingProperty(PropertyType type, InternalNode node, Object attribute);


	InternalEdge createExistingRelationship(RelationshipType type, InternalNode start, InternalNode end);
	
	InternalEdge createExistingRelationship(long id, RelationshipType type, InternalNode start, InternalNode end);


}
