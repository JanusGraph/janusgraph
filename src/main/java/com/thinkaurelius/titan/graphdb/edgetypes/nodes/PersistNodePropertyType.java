package com.thinkaurelius.titan.graphdb.edgetypes.nodes;

import com.thinkaurelius.titan.core.PropertyIndex;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.PropertyTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemPropertyType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public class PersistNodePropertyType extends PersistNodeEdgeType implements PropertyType {

	public PersistNodePropertyType(GraphTx g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistNodePropertyType(GraphTx g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}
	
	private PropertyTypeDefinition definition = null;
	
	@Override
	public PropertyTypeDefinition getDefinition() {
		if (definition==null) {
			definition = EdgeQueryUtil.queryHiddenFunctionalProperty(this, SystemPropertyType.PropertyTypeDefinition)
					.getAttribute(PropertyTypeDefinition.class);
		}
		return definition;
	}

	@Override
	public PropertyIndex getIndexType() {
		return getDefinition().getIndexType();
	}

	@Override
	public boolean isKeyed() {
		return getDefinition().isKeyed();
	}

	@Override
	public Class<?> getDataType() {
		return getDefinition().getDataType();
	}
	
	@Override
	public final boolean isPropertyType() {
		return true;
	}
	
	@Override
	public final boolean isRelationshipType() {
		return false;
	}

}
