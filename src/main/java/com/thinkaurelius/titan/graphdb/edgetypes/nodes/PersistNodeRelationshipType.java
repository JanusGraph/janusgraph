package com.thinkaurelius.titan.graphdb.edgetypes.nodes;

import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.RelationshipTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemPropertyType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public class PersistNodeRelationshipType extends PersistNodeEdgeType implements RelationshipType {

	public PersistNodeRelationshipType(GraphTx g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistNodeRelationshipType(GraphTx g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}
	
	private RelationshipTypeDefinition definition = null;
	
	@Override
	public RelationshipTypeDefinition getDefinition() {
		if (definition==null) {
			definition = EdgeQueryUtil.queryHiddenFunctionalProperty(this, SystemPropertyType.RelationshipTypeDefinition)
							.getAttribute(RelationshipTypeDefinition.class);
		}
		return definition;
	}
	
	
	@Override
	public final boolean isPropertyType() {
		return false;
	}
	
	@Override
	public final boolean isRelationshipType() {
		return true;
	}

}
