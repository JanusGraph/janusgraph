package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.types.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public class PersistVertexTitanLabel extends PersistVertexTitanType implements TitanLabel {

	public PersistVertexTitanLabel(InternalTitanTransaction g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistVertexTitanLabel(InternalTitanTransaction g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}
	
	private EdgeLabelDefinition definition = null;
	
	@Override
	public EdgeLabelDefinition getDefinition() {
		if (definition==null) {
			definition = QueryUtil.queryHiddenFunctionalProperty(this, SystemKey.RelationshipTypeDefinition)
							.getAttribute(EdgeLabelDefinition.class);
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
