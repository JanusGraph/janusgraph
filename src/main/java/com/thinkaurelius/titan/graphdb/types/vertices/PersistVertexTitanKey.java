package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.types.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public class PersistVertexTitanKey extends PersistVertexTitanType implements TitanKey {

	public PersistVertexTitanKey(InternalTitanTransaction g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistVertexTitanKey(InternalTitanTransaction g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}
	
	private PropertyKeyDefinition definition = null;
	
	@Override
	public PropertyKeyDefinition getDefinition() {
		if (definition==null) {
			definition = QueryUtil.queryHiddenFunctionalProperty(this, SystemKey.PropertyTypeDefinition)
					.getAttribute(PropertyKeyDefinition.class);
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
