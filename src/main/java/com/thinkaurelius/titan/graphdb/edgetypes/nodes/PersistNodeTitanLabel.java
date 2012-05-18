package com.thinkaurelius.titan.graphdb.edgetypes.nodes;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.RelationshipTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public class PersistNodeTitanLabel extends PersistNodeTitanType implements TitanLabel {

	public PersistNodeTitanLabel(InternalTitanTransaction g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistNodeTitanLabel(InternalTitanTransaction g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}
	
	private RelationshipTypeDefinition definition = null;
	
	@Override
	public RelationshipTypeDefinition getDefinition() {
		if (definition==null) {
			definition = EdgeQueryUtil.queryHiddenFunctionalProperty(this, SystemKey.RelationshipTypeDefinition)
							.getAttribute(RelationshipTypeDefinition.class);
		}
		return definition;
	}
	
	
	@Override
	public final boolean isPropertyKey() {
		return false;
	}
	
	@Override
	public final boolean isEdgeLabel() {
		return true;
	}

}
