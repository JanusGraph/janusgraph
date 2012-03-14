package com.thinkaurelius.titan.graphdb.edgetypes.nodes;

import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.EdgeCategory;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemPropertyType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.PersistDualNode;

public abstract class PersistNodeEdgeType extends PersistDualNode implements InternalEdgeType {

	public PersistNodeEdgeType(GraphTx g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistNodeEdgeType(GraphTx g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}

	private String name = null;
	
	@Override
	public String getName() {
		if (name==null) {
			name = EdgeQueryUtil.queryHiddenFunctionalProperty(this, SystemPropertyType.EdgeTypeName)
					.getAttribute(String.class);
		}
		return name;
	}
	
	@Override
	public EdgeCategory getCategory() {
		return getDefinition().getCategory();
	}

	@Override
	public Directionality getDirectionality() {
		return getDefinition().getDirectionality();
	}

	@Override
	public EdgeTypeGroup getGroup() {
		return getDefinition().getGroup();
	}

	@Override
	public boolean isFunctional() {
		return getDefinition().isFunctional();
	}

	@Override
	public boolean isHidden() {
		return getDefinition().isHidden();
	}

	@Override
	public boolean isModifiable() {
		return getDefinition().isModifiable();
	}


	
	
	
}
