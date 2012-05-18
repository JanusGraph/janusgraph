package com.thinkaurelius.titan.graphdb.edgetypes.nodes;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.PropertyTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public class PersistNodeTitanKey extends PersistNodeTitanType implements TitanKey {

	public PersistNodeTitanKey(InternalTitanTransaction g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistNodeTitanKey(InternalTitanTransaction g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}
	
	private PropertyTypeDefinition definition = null;
	
	@Override
	public PropertyTypeDefinition getDefinition() {
		if (definition==null) {
			definition = EdgeQueryUtil.queryHiddenFunctionalProperty(this, SystemKey.PropertyTypeDefinition)
					.getAttribute(PropertyTypeDefinition.class);
            assert definition!=null;
		}
		return definition;
	}

	@Override
	public boolean hasIndex() {
		return getDefinition().hasIndex();
	}

	@Override
	public boolean isUnique() {
		return getDefinition().isUnique();
	}

	@Override
	public Class<?> getDataType() {
		return getDefinition().getDataType();
	}
	
	@Override
	public final boolean isPropertyKey() {
		return true;
	}
	
	@Override
	public final boolean isEdgeLabel() {
		return false;
	}

}
