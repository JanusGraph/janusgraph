package com.thinkaurelius.titan.graphdb.edges.factory;

import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

/**
 * The logic to connect edges with vertices (i.e. add edges to the respective vertices' adjacency
 * list) remains outside of the EdgeFactory
 * @author matthias
 *
 */
public interface EdgeFactory extends EdgeLoader {

	InternalEdge createNewProperty(PropertyType type, InternalNode node, Object attribute);
	
	InternalEdge createNewRelationship(RelationshipType type, InternalNode start, InternalNode end);
	
	void setTransaction(GraphTx tx);
	

}
